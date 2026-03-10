pip install playwright pandas openpyxl tkinterdnd2
playwright install chromium

import ast
import csv
import hashlib
import json
import re
import sys
import time
import zipfile
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from playwright.sync_api import (
    BrowserContext,
    Download,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox
except Exception:
    tk = None
    filedialog = None
    messagebox = None


PROCESS_DIR_NAME = "secop_pdf"
CONTRACT_DIR_NAME = "contratos_pdf"
ERROR_SCREENSHOTS_DIR = "errores_capturas"
REPORT_CSV_NAME = "tiempos_por_documento.csv"
REPORT_XLSX_NAME = "tiempos_por_documento.xlsx"
STATE_FILE_NAME = "estado_secop.json"

DEFAULT_TIMEOUT = 15000
NAV_TIMEOUT = 45000
MAX_RETRIES = 3
SCROLL_STEPS = 8
SCROLL_PIXELS = 1200


@dataclass
class ProcessResult:
    index: int
    url: str
    notice_uid: str
    contract_id: str = ""
    process_pdf_saved: bool = False
    process_pdf_path: str = ""
    contract_pdfs_saved: int = 0
    contract_pdf_paths: str = ""
    elapsed_seconds: float = 0.0
    status: str = "pending"
    error_message: str = ""


def log(msg: str) -> None:
    print(msg, flush=True)


def choose_xlsx_file() -> Optional[Path]:
    if tk is None or filedialog is None:
        return None

    root = tk.Tk()
    root.withdraw()
    path = filedialog.askopenfilename(
        title="Selecciona el archivo Excel (.xlsx)",
        filetypes=[("Excel files", "*.xlsx")],
    )
    root.destroy()
    if not path:
        return None
    return Path(path).expanduser().resolve()


def safe_filename(value: str, max_len: int = 180) -> str:
    value = value.strip()
    value = re.sub(r"[\\/:*?\"<>|]+", "_", value)
    value = re.sub(r"\s+", " ", value)
    return value[:max_len]


def short_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:10]


def parse_urls_from_xlsx(xlsx_path: Path) -> List[str]:
    df = pd.read_excel(xlsx_path, dtype=str)
    first_col = df.iloc[:, 0]

    urls: List[str] = []
    for cell in first_col:
        if pd.isna(cell):
            continue

        cell = str(cell).strip()
        if not cell:
            continue

        url = None
        try:
            data = ast.literal_eval(cell)
            if isinstance(data, dict):
                url = data.get("url")
        except Exception:
            if cell.startswith("http://") or cell.startswith("https://"):
                url = cell

        if url:
            urls.append(url)

    seen = set()
    unique_urls = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)

    return unique_urls


def extract_notice_uid(url: str) -> str:
    match = re.search(r"noticeUID=([^&]+)", url)
    return match.group(1) if match else f"sin_noticeuid_{short_hash(url)}"


def zip_folder(folder_path: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in folder_path.rglob("*"):
            if file_path.is_file():
                zf.write(file_path, arcname=file_path.relative_to(folder_path.parent))


def save_state(state_file: Path, state: Dict) -> None:
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def load_state(state_file: Path) -> Dict:
    if not state_file.exists():
        return {"completed_notice_uids": [], "results": []}
    try:
        return json.loads(state_file.read_text(encoding="utf-8"))
    except Exception:
        return {"completed_notice_uids": [], "results": []}


def append_or_replace_result(results: List[Dict], new_result: Dict) -> List[Dict]:
    replaced = False
    for i, row in enumerate(results):
        if row.get("notice_uid") == new_result.get("notice_uid"):
            results[i] = new_result
            replaced = True
            break
    if not replaced:
        results.append(new_result)
    return results


def write_reports(output_dir: Path, results: List[Dict]) -> None:
    df = pd.DataFrame(results)

    csv_path = output_dir / REPORT_CSV_NAME
    xlsx_path = output_dir / REPORT_XLSX_NAME

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)


def take_error_screenshot(page: Page, screenshot_dir: Path, prefix: str) -> Optional[Path]:
    try:
        screenshot_dir.mkdir(parents=True, exist_ok=True)
        filename = safe_filename(f"{prefix}_{int(time.time())}.png")
        dest = screenshot_dir / filename
        page.screenshot(path=str(dest), full_page=True)
        return dest
    except Exception:
        return None


def body_text(page: Page, timeout: int = 5000) -> str:
    try:
        return page.locator("body").inner_text(timeout=timeout)
    except Exception:
        return ""


def wait_for_manual_captcha_resolution(page: Page) -> None:
    text = body_text(page).lower()
    captcha_indicators = [
        "i'm not a robot",
        "no soy un robot",
        "recaptcha",
        "complete la validación",
        "complete the validation",
    ]
    if any(x in text for x in captcha_indicators):
        log("\n[CAPTCHA] Detectado. Resuélvelo manualmente en el navegador.")
        input("Cuando termines, presiona ENTER para continuar... ")


def wait_until_secop_content(page: Page) -> None:
    wait_for_manual_captcha_resolution(page)

    targets = [
        "INFORMACIÓN DEL PROCEDIMIENTO",
        "Información",
        "Datos del contrato",
        "Información de la selección",
    ]
    for target in targets:
        try:
            page.get_by_text(target, exact=False).wait_for(timeout=10000)
            return
        except PlaywrightTimeoutError:
            continue

    time.sleep(2)


def find_first_visible(page: Page, selectors: List[Tuple[str, str]]) -> Optional[object]:
    """
    selectors: [(mode, value)] con mode in {"text", "role_button", "role_link", "locator"}
    """
    for mode, value in selectors:
        try:
            if mode == "text":
                locator = page.get_by_text(value, exact=False)
            elif mode == "role_button":
                locator = page.get_by_role("button", name=re.compile(value, re.I))
            elif mode == "role_link":
                locator = page.get_by_role("link", name=re.compile(value, re.I))
            else:
                locator = page.locator(value)

            count = locator.count()
            if count > 0:
                return locator.first
        except Exception:
            continue
    return None


def do_scroll(page: Page, steps: int = SCROLL_STEPS) -> None:
    for _ in range(steps):
        page.mouse.wheel(0, SCROLL_PIXELS)
        time.sleep(0.8)


def expect_download_with_retry(page: Page, click_fn, timeout: int = 25000) -> Optional[Download]:
    try:
        with page.expect_download(timeout=timeout) as info:
            click_fn()
        return info.value
    except Exception:
        return None


def download_process_pdf(page: Page, process_dir: Path, notice_uid: str) -> Tuple[bool, str]:
    btn = find_first_visible(page, [
        ("role_button", r"imprimir"),
        ("text", "Imprimir"),
        ("locator", "text=Imprimir"),
    ])
    if btn is None:
        return False, ""

    download = expect_download_with_retry(page, lambda: btn.click(timeout=DEFAULT_TIMEOUT))
    if not download:
        return False, ""

    dest = process_dir / f"{notice_uid}.pdf"
    download.save_as(str(dest))
    return True, str(dest)


def open_contract_view(page: Page) -> bool:
    try:
        do_scroll(page, steps=6)
    except Exception:
        pass

    link = find_first_visible(page, [
        ("role_link", r"ver contrato"),
        ("text", "Ver contrato"),
        ("locator", "text=Ver contrato"),
    ])
    if link is None:
        return False

    link.click(timeout=DEFAULT_TIMEOUT)
    time.sleep(2)
    return True


def get_contract_id(page: Page) -> Optional[str]:
    text = body_text(page)
    match = re.search(r"CO1\.PCCNTR\.\d+", text)
    return match.group(0) if match else None


def go_to_contract_documents(page: Page) -> bool:
    tab = find_first_visible(page, [
        ("role_link", r"documentos del contrato"),
        ("text", "Documentos del contrato"),
        ("locator", "text=Documentos del contrato"),
    ])
    if tab is None:
        return False

    tab.click(timeout=DEFAULT_TIMEOUT)
    time.sleep(2)
    return True


def collect_download_links(page: Page) -> List[object]:
    links = []

    for candidate in [
        page.get_by_role("link", name=re.compile(r"descargar", re.I)),
        page.get_by_text("Descargar", exact=False),
        page.locator("text=Descargar"),
    ]:
        try:
            count = candidate.count()
            for i in range(count):
                links.append(candidate.nth(i))
        except Exception:
            continue

    # deduplicar por texto + posición tentativa
    unique = []
    seen = set()
    for link in links:
        try:
            txt = link.inner_text(timeout=1000)
        except Exception:
            txt = "descargar"
        key = txt
        if key not in seen:
            seen.add(key)
            unique.append(link)

    return unique


def guess_download_filename(download: Download, contract_id: str, index: int) -> str:
    suggested = safe_filename(download.suggested_filename or f"{contract_id}_{index}.pdf")
    suffix = Path(suggested).suffix.lower()
    if suffix != ".pdf":
        suggested = f"{Path(suggested).stem}.pdf"

    if index == 1:
        return f"{contract_id}.pdf"
    return f"{contract_id}_{index}.pdf"


def download_all_contract_documents(page: Page, contract_dir: Path, contract_id: str) -> List[str]:
    downloaded_paths: List[str] = []
    links = collect_download_links(page)

    if not links:
        return downloaded_paths

    for idx, link in enumerate(links, start=1):
        try:
            download = expect_download_with_retry(
                page,
                lambda lnk=link: lnk.click(timeout=DEFAULT_TIMEOUT),
                timeout=20000,
            )
            if not download:
                continue

            filename = guess_download_filename(download, contract_id, idx)
            dest = contract_dir / filename

            # evita sobreescrituras si ya existe
            if dest.exists():
                stem = dest.stem
                suffix = dest.suffix
                dest = contract_dir / f"{stem}_{int(time.time())}{suffix}"

            download.save_as(str(dest))
            downloaded_paths.append(str(dest))
            time.sleep(1)
        except Exception:
            continue

    return downloaded_paths


def process_one_url(
    context: BrowserContext,
    page: Page,
    url: str,
    index: int,
    process_dir: Path,
    contract_dir: Path,
    screenshot_dir: Path,
) -> ProcessResult:
    notice_uid = extract_notice_uid(url)
    result = ProcessResult(index=index, url=url, notice_uid=notice_uid)

    start = time.perf_counter()

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        time.sleep(2)
        wait_until_secop_content(page)

        ok_process, process_path = download_process_pdf(page, process_dir, notice_uid)
        result.process_pdf_saved = ok_process
        result.process_pdf_path = process_path

        do_scroll(page)

        if open_contract_view(page):
            contract_id = get_contract_id(page)
            if contract_id:
                result.contract_id = contract_id

            if go_to_contract_documents(page):
                contract_id_final = result.contract_id or f"contrato_{notice_uid}"
                downloaded = download_all_contract_documents(page, contract_dir, contract_id_final)
                result.contract_pdfs_saved = len(downloaded)
                result.contract_pdf_paths = " | ".join(downloaded)

        result.status = "ok"

    except Exception as exc:
        result.status = "error"
        result.error_message = str(exc)
        take_error_screenshot(page, screenshot_dir, f"{notice_uid}_error")

    result.elapsed_seconds = round(time.perf_counter() - start, 2)
    return result


def process_with_retries(
    context: BrowserContext,
    page: Page,
    url: str,
    index: int,
    process_dir: Path,
    contract_dir: Path,
    screenshot_dir: Path,
    max_retries: int = MAX_RETRIES,
) -> ProcessResult:
    last_result: Optional[ProcessResult] = None

    for attempt in range(1, max_retries + 1):
        log(f"\n[{index}] Intento {attempt}/{max_retries} -> {extract_notice_uid(url)}")
        result = process_one_url(
            context=context,
            page=page,
            url=url,
            index=index,
            process_dir=process_dir,
            contract_dir=contract_dir,
            screenshot_dir=screenshot_dir,
        )
        last_result = result

        if result.status == "ok":
            return result

        log(f"  [WARN] Falló intento {attempt}: {result.error_message}")
        time.sleep(3)

    assert last_result is not None
    return last_result


def main():
    # Entrada
    if len(sys.argv) >= 2:
        xlsx_path = Path(sys.argv[1]).expanduser().resolve()
    else:
        xlsx_path = choose_xlsx_file()
        if xlsx_path is None:
            print("No se seleccionó archivo Excel.")
            sys.exit(1)

    output_dir = Path(sys.argv[2]).expanduser().resolve() if len(sys.argv) >= 3 else Path.cwd() / "salida_secop_v2"

    if not xlsx_path.exists():
        print(f"No existe el archivo: {xlsx_path}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    process_dir = output_dir / PROCESS_DIR_NAME
    contract_dir = output_dir / CONTRACT_DIR_NAME
    screenshot_dir = output_dir / ERROR_SCREENSHOTS_DIR
    state_file = output_dir / STATE_FILE_NAME

    process_dir.mkdir(exist_ok=True)
    contract_dir.mkdir(exist_ok=True)
    screenshot_dir.mkdir(exist_ok=True)

    urls = parse_urls_from_xlsx(xlsx_path)
    if not urls:
        print("No se encontraron URLs válidas en el archivo.")
        sys.exit(1)

    state = load_state(state_file)
    completed_notice_uids = set(state.get("completed_notice_uids", []))
    results = state.get("results", [])

    log(f"Archivo Excel: {xlsx_path}")
    log(f"URLs encontradas: {len(urls)}")
    log(f"Salida: {output_dir}")
    log(f"Ya completados: {len(completed_notice_uids)}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, downloads_path=str(output_dir))
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        for idx, url in enumerate(urls, start=1):
            notice_uid = extract_notice_uid(url)
            if notice_uid in completed_notice_uids:
                log(f"\n[{idx}] Saltando {notice_uid} (ya procesado)")
                continue

            result = process_with_retries(
                context=context,
                page=page,
                url=url,
                index=idx,
                process_dir=process_dir,
                contract_dir=contract_dir,
                screenshot_dir=screenshot_dir,
            )

            results = append_or_replace_result(results, asdict(result))

            # marca como completado aunque haya error para evitar loops infinitos
            completed_notice_uids.add(notice_uid)
            state["completed_notice_uids"] = sorted(completed_notice_uids)
            state["results"] = results
            save_state(state_file, state)

            write_reports(output_dir, results)

        context.close()
        browser.close()

    process_zip = output_dir / f"{PROCESS_DIR_NAME}.zip"
    contract_zip = output_dir / f"{CONTRACT_DIR_NAME}.zip"

    zip_folder(process_dir, process_zip)
    zip_folder(contract_dir, contract_zip)
    write_reports(output_dir, results)

    log("\nProceso terminado.")
    log(f"ZIP procesos:  {process_zip}")
    log(f"ZIP contratos: {contract_zip}")
    log(f"CSV reporte:   {output_dir / REPORT_CSV_NAME}")
    log(f"XLSX reporte:  {output_dir / REPORT_XLSX_NAME}")
    log(f"Estado:        {state_file}")


if __name__ == "__main__":
    main()
