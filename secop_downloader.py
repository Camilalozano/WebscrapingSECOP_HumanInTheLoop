pip install playwright pandas openpyxl
playwright install chromium

import ast
import csv
import re
import sys
import time
import zipfile
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from playwright.sync_api import (
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)


PROCESS_DIR_NAME = "secop_pdf"
CONTRACT_DIR_NAME = "contratos_pdf"
REPORT_NAME = "tiempos_por_documento.csv"


def slugify_filename(value: str) -> str:
    value = value.strip()
    value = re.sub(r"[\\/:*?\"<>|]+", "_", value)
    value = re.sub(r"\s+", " ", value)
    return value[:180]


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

    # quita duplicados preservando orden
    seen = set()
    unique_urls = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)

    return unique_urls


def extract_notice_uid(url: str) -> str:
    match = re.search(r"noticeUID=([^&]+)", url)
    return match.group(1) if match else f"sin_noticeuid_{int(time.time())}"


def wait_for_manual_captcha_resolution(page) -> None:
    """
    Si aparece captcha, pausa hasta que el usuario lo resuelva.
    """
    page_text = page.locator("body").inner_text(timeout=5000).lower()

    captcha_indicators = [
        "i'm not a robot",
        "no soy un robot",
        "recaptcha",
        "complete la validación",
        "complete the validation",
    ]

    if any(indicator in page_text for indicator in captcha_indicators):
        print("\n[CAPTCHA] Detectado reCAPTCHA.")
        print("Resuélvelo manualmente en la ventana del navegador.")
        input("Cuando termines, presiona ENTER aquí para continuar... ")


def wait_until_page_is_accessible(page) -> None:
    """
    Espera a que el contenido real del SECOP esté visible.
    """
    wait_for_manual_captcha_resolution(page)

    # Intento de espera por contenido típico
    candidates = [
        "INFORMACIÓN DEL PROCEDIMIENTO",
        "Información",
        "Datos del contrato",
        "Información de la selección",
    ]

    for text in candidates:
        try:
            page.get_by_text(text, exact=False).wait_for(timeout=10000)
            return
        except PlaywrightTimeoutError:
            continue

    # Último intento, si ya cargó algo útil
    time.sleep(2)


def safe_click(locator, timeout: int = 10000) -> bool:
    try:
        locator.wait_for(timeout=timeout)
        locator.click(timeout=timeout)
        return True
    except Exception:
        return False


def find_first(locator_candidates) -> Optional[object]:
    for locator in locator_candidates:
        try:
            if locator.count() > 0:
                return locator.first
        except Exception:
            continue
    return None


def download_process_pdf(page, process_dir: Path, notice_uid: str) -> Optional[Path]:
    """
    Descarga el PDF del proceso usando el botón 'Imprimir'.
    """
    print(f"  - Descargando PDF del proceso para {notice_uid}...")

    button_candidates = [
        page.get_by_role("button", name=re.compile(r"imprimir", re.I)),
        page.get_by_text("Imprimir", exact=False),
        page.locator("text=Imprimir"),
    ]

    btn = find_first(button_candidates)
    if btn is None:
        print("    [WARN] No encontré el botón 'Imprimir'.")
        return None

    try:
        with page.expect_download(timeout=20000) as download_info:
            btn.click(timeout=10000)
        download = download_info.value
        dest = process_dir / f"{notice_uid}.pdf"
        download.save_as(str(dest))
        print(f"    [OK] Guardado: {dest.name}")
        return dest
    except PlaywrightTimeoutError:
        print("    [WARN] No se produjo descarga al hacer clic en 'Imprimir'.")
        return None
    except Exception as exc:
        print(f"    [WARN] Error descargando PDF del proceso: {exc}")
        return None


def open_contract_modal(page) -> bool:
    """
    Busca 'Información de la selección' y hace clic en 'Ver contrato'.
    """
    print("  - Buscando 'Ver contrato'...")
    try:
        page.get_by_text("Información de la selección", exact=False).scroll_into_view_if_needed(timeout=10000)
    except Exception:
        pass

    link_candidates = [
        page.get_by_role("link", name=re.compile(r"ver contrato", re.I)),
        page.get_by_text("Ver contrato", exact=False),
        page.locator("text=Ver contrato"),
    ]

    link = find_first(link_candidates)
    if link is None:
        print("    [WARN] No encontré el enlace 'Ver contrato'.")
        return False

    try:
        link.click(timeout=10000)
        time.sleep(2)
        return True
    except Exception as exc:
        print(f"    [WARN] No pude abrir 'Ver contrato': {exc}")
        return False


def get_contract_id(page) -> Optional[str]:
    """
    Intenta extraer el ID del contrato visible en la pantalla.
    Ejemplo: CO1.PCCNTR.7295345
    """
    try:
        text = page.locator("body").inner_text(timeout=5000)
    except Exception:
        return None

    match = re.search(r"CO1\.PCCNTR\.\d+", text)
    return match.group(0) if match else None


def go_to_contract_documents_tab(page) -> bool:
    """
    Intenta abrir la pestaña 'Documentos del contrato'.
    """
    tab_candidates = [
        page.get_by_role("link", name=re.compile(r"documentos del contrato", re.I)),
        page.get_by_text("Documentos del contrato", exact=False),
        page.locator("text=Documentos del contrato"),
    ]

    tab = find_first(tab_candidates)
    if tab is None:
        print("    [WARN] No encontré la pestaña 'Documentos del contrato'.")
        return False

    try:
        tab.click(timeout=10000)
        time.sleep(2)
        return True
    except Exception as exc:
        print(f"    [WARN] No pude abrir 'Documentos del contrato': {exc}")
        return False


def download_contract_pdf(page, contract_dir: Path, default_name: str) -> Tuple[Optional[Path], Optional[str]]:
    """
    Descarga el PDF del contrato desde 'Documentos del contrato'.
    Devuelve: (ruta_pdf, contract_id)
    """
    print("  - Descargando PDF del contrato...")

    contract_id = get_contract_id(page)

    # Busca cualquier link o botón "Descargar"
    download_candidates = [
        page.get_by_role("link", name=re.compile(r"descargar", re.I)),
        page.get_by_text("Descargar", exact=False),
        page.locator("text=Descargar"),
    ]

    download_link = find_first(download_candidates)
    if download_link is None:
        print("    [WARN] No encontré enlace 'Descargar' del contrato.")
        return None, contract_id

    try:
        with page.expect_download(timeout=20000) as download_info:
            download_link.click(timeout=10000)
        download = download_info.value

        final_name = contract_id if contract_id else default_name
        dest = contract_dir / f"{final_name}.pdf"
        download.save_as(str(dest))
        print(f"    [OK] Guardado: {dest.name}")
        return dest, contract_id
    except PlaywrightTimeoutError:
        print("    [WARN] No se produjo descarga del contrato.")
        return None, contract_id
    except Exception as exc:
        print(f"    [WARN] Error descargando contrato: {exc}")
        return None, contract_id


def zip_folder(folder_path: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in folder_path.rglob("*"):
            if file_path.is_file():
                zf.write(file_path, arcname=file_path.relative_to(folder_path.parent))


def write_report(report_path: Path, rows: List[dict]) -> None:
    fieldnames = [
        "index",
        "notice_uid",
        "contract_id",
        "url",
        "process_pdf_saved",
        "contract_pdf_saved",
        "elapsed_seconds",
    ]
    with open(report_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def process_url(page, url: str, process_dir: Path, contract_dir: Path, index: int) -> dict:
    notice_uid = extract_notice_uid(url)
    contract_id = None
    process_pdf_saved = False
    contract_pdf_saved = False

    print(f"\n[{index}] Procesando {notice_uid}")
    start = time.perf_counter()

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(2)

        wait_until_page_is_accessible(page)

        process_pdf_path = download_process_pdf(page, process_dir, notice_uid)
        process_pdf_saved = process_pdf_path is not None

        # Baja hasta la zona de "Información de la selección"
        try:
            for _ in range(6):
                body_text = page.locator("body").inner_text(timeout=3000)
                if "Información de la selección" in body_text:
                    break
                page.mouse.wheel(0, 1200)
                time.sleep(1)
        except Exception:
            pass

        if open_contract_modal(page):
            time.sleep(2)

            # A veces abre modal; a veces cambia contenido en la misma vista.
            contract_id = get_contract_id(page)

            if go_to_contract_documents_tab(page):
                contract_pdf_path, contract_id_detected = download_contract_pdf(
                    page=page,
                    contract_dir=contract_dir,
                    default_name=f"contrato_{notice_uid}",
                )
                if contract_id_detected:
                    contract_id = contract_id_detected
                contract_pdf_saved = contract_pdf_path is not None

    except Exception as exc:
        print(f"  [ERROR] Falló {notice_uid}: {exc}")

    elapsed = round(time.perf_counter() - start, 2)

    return {
        "index": index,
        "notice_uid": notice_uid,
        "contract_id": contract_id or "",
        "url": url,
        "process_pdf_saved": process_pdf_saved,
        "contract_pdf_saved": contract_pdf_saved,
        "elapsed_seconds": elapsed,
    }


def main():
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python secop_downloader.py archivo.xlsx [directorio_salida]")
        sys.exit(1)

    xlsx_path = Path(sys.argv[1]).expanduser().resolve()
    output_dir = Path(sys.argv[2]).expanduser().resolve() if len(sys.argv) > 2 else Path.cwd() / "salida_secop"

    if not xlsx_path.exists():
        print(f"No existe el archivo: {xlsx_path}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    process_dir = output_dir / PROCESS_DIR_NAME
    contract_dir = output_dir / CONTRACT_DIR_NAME
    process_dir.mkdir(exist_ok=True)
    contract_dir.mkdir(exist_ok=True)

    urls = parse_urls_from_xlsx(xlsx_path)
    if not urls:
        print("No encontré URLs válidas en el Excel.")
        sys.exit(1)

    print(f"Se encontraron {len(urls)} URLs.")
    print(f"Salida: {output_dir}")

    report_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,   # visible para que puedas resolver captcha
            downloads_path=str(output_dir),
        )
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        for idx, url in enumerate(urls, start=1):
            row = process_url(
                page=page,
                url=url,
                process_dir=process_dir,
                contract_dir=contract_dir,
                index=idx,
            )
            report_rows.append(row)

        context.close()
        browser.close()

    report_path = output_dir / REPORT_NAME
    write_report(report_path, report_rows)

    process_zip = output_dir / f"{PROCESS_DIR_NAME}.zip"
    contract_zip = output_dir / f"{CONTRACT_DIR_NAME}.zip"

    zip_folder(process_dir, process_zip)
    zip_folder(contract_dir, contract_zip)

    print("\nProceso terminado.")
    print(f"ZIP procesos:  {process_zip}")
    print(f"ZIP contratos: {contract_zip}")
    print(f"Reporte:       {report_path}")


if __name__ == "__main__":
    main()
