"""Build the README media from two committed sources, no browser, no network:

  docs/assets/brand/ghost-grid.png   the pixel master of the mascot, 112x100, one colour + alpha; edit pixel by pixel

Outputs (docs/assets/): hero-static.png + hero.gif (1280x480, 3.5 s loop). flow.gif + flow-poster.png are a browser
recording of the explorer front-end (scripts/record_flow.js), re-encoded here: `--flow-frames DIR`.
Motion rule (same as the reference project): transform the master, never redraw it - the ghost only floats by row
shifts and blinks by masking the eye boxes; the wordmark never moves. Needs Pillow (`uv sync --extra media`) and
ffmpeg on PATH (GIF via palettegen/paletteuse, so no gifski). Nothing here reads .env.
"""
from __future__ import annotations

import math
import shutil
import subprocess
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent
ASSETS = ROOT / "docs" / "assets"
MASTER = ASSETS / "brand" / "ghost-grid.png"
TMP = Path("/tmp/rhflow-media")

BG, PANEL = (8, 6, 9), (19, 15, 24)
PINK, PINK_HI, PINK_DEEP = (0xEB, 0x25, 0xD5), (0xFB, 0xC5, 0xF3), (0x4A, 0x0F, 0x45)
TEXT, GREY, DIM = (0xF2, 0xEE, 0xF6), (0xA3, 0xA3, 0xA3), (0x61, 0x54, 0x68)
EYES = [((30, 38), (48, 50)), ((58, 68), (46, 50))]  # (gx range, gy range) in master pixels, from the monitor scene
HUB_MAX = 5_000_000

GLYPHS = {  # 5x5 block letters, the same shape family as the terminal wordmark
    "H": ["#   #", "#   #", "#####", "#   #", "#   #"],
    "G": [" ####", "#    ", "#  ##", "#   #", " ####"],
    "O": [" ### ", "#   #", "#   #", "#   #", " ### "],
    "S": [" ####", "#    ", " ### ", "    #", "#### "],
    "T": ["#####", "  #  ", "  #  ", "  #  ", "  #  "],
}
WORD = "GHOST"


def font(size: int) -> ImageFont.FreeTypeFont:
    for p in ("/System/Library/Fonts/Menlo.ttc", "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"):
        if Path(p).exists():
            return ImageFont.truetype(p, size)
    return ImageFont.load_default(size)


def nearest(im: Image.Image, k: int) -> Image.Image:
    return im.resize((im.width * k, im.height * k), Image.NEAREST)


def wordmark(cw: int, ch: int) -> Image.Image:
    rows = [" ".join(GLYPHS[c][i] for c in WORD) for i in range(5)]
    im = Image.new("RGBA", (len(rows[0]) * cw + cw, 5 * ch + ch), (0, 0, 0, 0))
    d = ImageDraw.Draw(im)
    for dy, dx, col in ((ch // 2, cw // 2, PINK_DEEP), (0, 0, PINK)):  # pressed shadow, then the letters
        for y, row in enumerate(rows):
            for x, c in enumerate(row):
                if c == "#":
                    d.rectangle([x * cw + dx, y * ch + dy, (x + 1) * cw - 1 + dx, (y + 1) * ch - 1 + dy], fill=col + (255,))
    return im


def ghost(master: Image.Image, bob: float, blink: bool) -> Image.Image:
    """Float: every row moves up by up to 3 px, the top rows a little more than the skirt, so the body breathes.
    Blink: the eye boxes are cut out (drawn in the page colour)."""
    w, h = master.size
    out = Image.new("RGBA", (w, h + 4), (0, 0, 0, 0))
    for y in range(h):
        dy = int(round(bob * (2 + 1 * (1 - y / h))))
        out.paste(master.crop((0, y, w, y + 1)), (0, y + 3 - dy))
    if blink:
        d = ImageDraw.Draw(out)
        for (x0, x1), (y0, y1) in EYES:
            d.rectangle([x0, y0 + 3, x1, y1 + 3], fill=BG + (255,))
    return out


def ease(t: float) -> float:
    return t * t * (3 - 2 * t)


def hero_frame(master: Image.Image, bob: float, blink: bool, stroke: float, f: ImageFont.FreeTypeFont) -> Image.Image:
    W, H = 1280, 480
    im = Image.new("RGBA", (W, H), BG + (255,))
    d = ImageDraw.Draw(im)
    wm = wordmark(12, 24)
    im.alpha_composite(wm, (72, 132))
    d.text((72, 132 + wm.height + 14), "fund-flow pipeline  ·  Robinhood Chain  ·  read-only", font=f, fill=GREY + (255,))
    y = 132 + wm.height + 58
    x_end = 72 + int(stroke * (700 - 72))
    if stroke > 0:
        d.rectangle([72, y, x_end, y + 3], fill=PINK + (255,))
        d.polygon([(x_end + 1, y - 4), (x_end + 11, y + 1), (x_end + 1, y + 7)], fill=PINK + (255,))
    sp = nearest(ghost(master, bob, blink), 4)
    im.alpha_composite(sp, (W - sp.width - 64, (H - sp.height) // 2))
    return im.convert("RGB")


def gif(frames_dir: Path, fps: int, out: Path) -> None:
    vf = "split[a][b];[a]palettegen=max_colors=64:stats_mode=diff[p];[b][p]paletteuse=dither=none"
    subprocess.run(["ffmpeg", "-v", "error", "-y", "-framerate", str(fps), "-i", str(frames_dir / "f-%03d.png"),
                    "-vf", vf, "-loop", "0", str(out)], check=True)


def fresh(name: str) -> Path:
    p = TMP / name
    shutil.rmtree(p, ignore_errors=True)
    p.mkdir(parents=True)
    return p


def build_hero() -> None:
    master = Image.open(MASTER).convert("RGBA")
    f = font(22)
    hero_frame(master, 0.0, False, 1.0, f).save(ASSETS / "hero-static.png", optimize=True)
    frames = fresh("hero")
    fps, dur = 8, 3.5
    for i in range(int(fps * dur)):
        t = i / fps
        bob = 0.5 - 0.5 * math.cos(2 * math.pi * t / dur)  # one full float per loop, seamless
        blink = 2.5 <= t < 2.75
        if t < 0.4:
            stroke = 0.0
        elif t < 1.1:
            stroke = ease((t - 0.4) / 0.7)
        elif t < 2.9:
            stroke = 1.0
        elif t < 3.3:
            stroke = 1 - ease((t - 2.9) / 0.4)
        else:
            stroke = 0.0
        hero_frame(master, bob, blink, stroke, f).save(frames / f"f-{i:03d}.png")
    gif(frames, fps, ASSETS / "hero.gif")


def build_flow(frames: Path) -> None:
    """flow.gif is a screen recording of the GHOST flow page (the front-end lives in its own repo), captured by
    scripts/record_flow.js into `frames` (PNG + list.txt with real durations). 30 fps, 1152 px wide, 96 colours:
    the trace stream on the right changes every frame, so 7 s is ~11.8 MB (9.9 MB at 20 fps); fewer colours or fps shrink it."""
    subprocess.run(["ffmpeg", "-v", "error", "-y", "-f", "concat", "-safe", "0", "-i", str(frames / "list.txt"), "-vf",
                    "fps=30,scale=1152:-1:flags=lanczos,split[a][b];[a]palettegen=max_colors=96:stats_mode=diff[p];"
                    "[b][p]paletteuse=dither=none", "-loop", "0", str(ASSETS / "flow.gif")], check=True)
    subprocess.run(["ffmpeg", "-v", "error", "-y", "-i", str(frames / "f-0100.png"), "-vf", "scale=1152:-1:flags=lanczos",
                    str(ASSETS / "flow-poster.png")], check=True)


def report() -> None:
    for p in sorted(ASSETS.glob("*.*")):
        print(f"{p.name:20s} {p.stat().st_size / 1024:8.0f} KB")


if __name__ == "__main__":
    if not shutil.which("ffmpeg"):
        sys.exit("ffmpeg not on PATH")
    build_hero()
    if len(sys.argv) == 3 and sys.argv[1] == "--flow-frames":
        build_flow(Path(sys.argv[2]))
    report()
