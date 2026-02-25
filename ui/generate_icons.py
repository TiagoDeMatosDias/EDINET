"""Generate .ico and .png from the SVG icon."""
import cairosvg
from PIL import Image
from io import BytesIO
from pathlib import Path

assets = Path(__file__).resolve().parent.parent / "assets"
svg_path = assets / "icon.svg"
svg_data = svg_path.read_bytes()

# Multi-resolution ICO
sizes = [16, 24, 32, 48, 64, 128, 256]
images = []
for sz in sizes:
    png_data = cairosvg.svg2png(bytestring=svg_data, output_width=sz, output_height=sz)
    img = Image.open(BytesIO(png_data)).convert("RGBA")
    images.append(img)

ico_path = assets / "icon.ico"
# Pillow ICO plugin: pass the largest image and request all sizes
# The plugin will down-scale from the source for each requested size.
biggest = max(images, key=lambda im: im.width)
biggest.save(
    str(ico_path),
    format="ICO",
    sizes=[(sz, sz) for sz in sizes],
)
print(f"Created: {ico_path} ({ico_path.stat().st_size} bytes)")

# 512px PNG for Flet window icon
png512 = cairosvg.svg2png(bytestring=svg_data, output_width=512, output_height=512)
png_path = assets / "icon.png"
png_path.write_bytes(png512)
print(f"Created: {png_path} ({png_path.stat().st_size} bytes)")
