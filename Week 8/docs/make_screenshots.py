"""
make_screenshots.py (one-off doc generation script, not part of the pipeline)
--------------------------------------------------------------------------------
Renders real captured console output from the pipeline as terminal-style
PNG "screenshots" for the README/screenshots folder. Uses PIL only.
"""

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

OUT_DIR = Path("/home/claude/ecommerce_analytics/screenshots")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BG = "#1e1e2e"
TITLEBAR = "#2a2a3d"
FG = "#d4d4dc"
GREEN = "#7ec699"
RED = "#e57373"
AMBER = "#e0af68"
DIM = "#8888a0"

FONT_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",
]


def get_font(size=15):
    for path in FONT_PATHS:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def colorize(line: str) -> str:
    """Rough colour tag for a line based on content, returns a colour hex."""
    stripped = line.strip()
    if stripped.startswith("PASS") or "PASS:" in line:
        return GREEN
    if stripped.startswith("FAIL") or stripped.startswith("ERROR"):
        return RED
    if "===" in line or "---" in line:
        return DIM
    if any(k in line for k in ("WARNING", "At Risk")):
        return AMBER
    return FG


def render_terminal(lines: list[str], out_path: Path, title: str, width: int = 980,
                     line_height: int = 21, font_size: int = 14, max_lines: int | None = None,
                     wrap_chars: int = 130) -> None:
    # Drop consecutive exact duplicates (e.g. a progress bar's last tick + its finish() print)
    deduped = []
    for l in lines:
        if not deduped or deduped[-1] != l:
            deduped.append(l)
    lines = deduped
    if max_lines:
        lines = lines[:max_lines]
    # Truncate overly long lines so they don't run off the canvas
    lines = [(l if len(l) <= wrap_chars else l[:wrap_chars - 1] + "…") for l in lines]
    font = get_font(font_size)
    pad_x, pad_top = 18, 44
    height = pad_top + line_height * len(lines) + 18

    img = Image.new("RGB", (width, height), BG)
    draw = ImageDraw.Draw(img)

    # Title bar with traffic-light dots
    draw.rectangle([0, 0, width, 34], fill=TITLEBAR)
    for i, dot_color in enumerate(["#ff5f56", "#ffbd2e", "#27c93f"]):
        draw.ellipse([14 + i * 22, 12, 24 + i * 22, 22], fill=dot_color)
    draw.text((width / 2, 17), title, font=get_font(13), fill="#c9c9d4", anchor="mm")

    y = pad_top
    for line in lines:
        color = colorize(line)
        draw.text((pad_x, y), line.rstrip("\n"), font=font, fill=color)
        y += line_height

    img.save(out_path)
    print(f"Wrote {out_path}")


def load_lines(path: str) -> list[str]:
    return Path(path).read_text(encoding="utf-8").splitlines()


if __name__ == "__main__":
    # 1. Data generation run — keep milestone lines only (drop per-percent progress ticks)
    gen_lines_raw = load_lines("/tmp/gen_output.txt")
    gen_clean = []
    for l in gen_lines_raw:
        if not l.strip():
            continue
        if "%" in l and ("[" in l and "]" in l) and not l.strip().endswith("100%"):
            continue  # skip intermediate progress-bar ticks, keep only the final 100% line
        gen_clean.append(l)
    render_terminal(gen_clean, OUT_DIR / "01_generate_data.png",
                     "python generate_data.py", width=1180, font_size=13, max_lines=14)

    # 2. Test suite run
    test_lines = load_lines("/tmp/test_output.txt")
    render_terminal(test_lines, OUT_DIR / "02_test_suite.png",
                     "python tests/test_edge_cases.py", width=1180, font_size=13)

    # 3. CLI report
    cli_lines = load_lines("/tmp/cli_output.txt")
    render_terminal(cli_lines[:22], OUT_DIR / "03_cli_report.png",
                     "python cli.py", width=1180, font_size=13)
