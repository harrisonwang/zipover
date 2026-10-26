#!/usr/bin/env python3
"""Generate application icons from assets/tray-icon.svg.

Requires Python 3, Pillow, and librsvg's rsvg-convert.
"""

from copy import deepcopy
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile
import xml.etree.ElementTree as ET

from PIL import Image


ASSETS = Path(__file__).resolve().parents[1] / "assets"
SVG = "http://www.w3.org/2000/svg"
ICO_SIZES = (16, 20, 24, 32, 40, 48, 64, 128, 256)
ICNS_SIZES = {
    b"icp4": 16,
    b"ic11": 32,  # 16 pt @2x
    b"icp5": 32,
    b"ic12": 64,  # 32 pt @2x
    b"ic07": 128,
    b"ic13": 256,  # 128 pt @2x
    b"ic08": 256,
    b"ic14": 512,  # 256 pt @2x
    b"ic09": 512,
    b"ic10": 1024,  # 512 pt @2x
}
ET.register_namespace("", SVG)


def serialize(root):
    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="unicode") + "\n"


def application_svg(symbol):
    root = ET.fromstring('''<svg xmlns="http://www.w3.org/2000/svg" width="1024" height="1024" viewBox="0 0 1024 1024" role="img" aria-labelledby="title desc">
      <title id="title">Zipover application icon</title>
      <desc id="desc">A white Z on a cobalt blue rounded square.</desc>
      <defs>
        <linearGradient id="tile" x1="0" y1="0" x2="0.8" y2="1">
          <stop stop-color="#4d7af0"/>
          <stop offset="1" stop-color="#244ec9"/>
        </linearGradient>
        <filter id="shadow" x="-20%" y="-20%" width="140%" height="150%">
          <feDropShadow dx="0" dy="18" stdDeviation="18" flood-color="#142445" flood-opacity="0.19"/>
        </filter>
      </defs>
      <rect x="64" y="64" width="896" height="896" rx="206" fill="url(#tile)" filter="url(#shadow)"/>
      <rect x="66" y="66" width="892" height="892" rx="204" fill="none" stroke="#ffffff" stroke-opacity="0.18" stroke-width="4"/>
      <g transform="translate(158 158) scale(29.5)" color="#ffffff"/>
    </svg>''')
    mark = root.find(f"{{{SVG}}}g")
    for child in symbol:
        if child.tag not in (f"{{{SVG}}}title", f"{{{SVG}}}desc"):
            mark.append(deepcopy(child))
    return serialize(root)


def render(svg, path, size):
    subprocess.run(
        ["rsvg-convert", "--width", str(size), "--height", str(size), "--output", str(path)],
        input=svg.encode(),
        check=True,
    )


def main():
    if shutil.which("rsvg-convert") is None:
        raise SystemExit("Missing rsvg-convert; see assets/README.md for requirements.")

    source = (ASSETS / "tray-icon.svg").read_text()
    symbol = ET.fromstring(source)
    if symbol.get("viewBox") != "0 0 24 24":
        raise SystemExit("The master symbol must use a 24 × 24 viewBox.")
    desktop = application_svg(symbol)
    symbolic = deepcopy(symbol)
    symbolic.set("color", "#2e3436")

    with tempfile.TemporaryDirectory(prefix="zipover-icons-") as directory:
        stage = Path(directory)
        (stage / "zipover.svg").write_text(desktop)
        (stage / "zipover-symbolic.svg").write_text(serialize(symbolic))
        # macOS uses the alpha as a template; Windows displays the blue mark.
        render(source, stage / "tray-icon.png", 36)

        sizes = sorted(set(ICO_SIZES) | set(ICNS_SIZES.values()))
        for size in sizes:
            render(desktop, stage / f"app-{size}.png", size)
        shutil.copyfile(stage / "app-512.png", stage / "zipover.png")

        # Use vector renders at every size, including intermediate Windows DPI sizes.
        frames = []
        for size in ICO_SIZES:
            with Image.open(stage / f"app-{size}.png") as image:
                frames.append(image.convert("RGBA"))
        frames[-1].save(
            stage / "zipover.ico",
            format="ICO",
            sizes=[(size, size) for size in ICO_SIZES],
            append_images=frames[:-1],
        )

        # Modern ICNS wraps PNG representations in typed, length-prefixed chunks.
        chunks = []
        for kind, size in ICNS_SIZES.items():
            png = (stage / f"app-{size}.png").read_bytes()
            chunks.append(struct.pack(">4sI", kind, len(png) + 8) + png)
        payload = b"".join(chunks)
        (stage / "zipover.icns").write_bytes(
            struct.pack(">4sI", b"icns", len(payload) + 8) + payload
        )

        for name in ("tray-icon.png", "zipover-symbolic.svg", "zipover.svg", "zipover.png", "zipover.ico", "zipover.icns"):
            shutil.copyfile(stage / name, ASSETS / name)
            print(f"Generated assets/{name}")


if __name__ == "__main__":
    main()
