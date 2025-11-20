#!/usr/bin/env python3
"""
Script to generate PWA icons for Plex Notifier.
This creates placeholder icons - replace with your actual design.
"""

from PIL import Image, ImageDraw, ImageFont
import os

# Plex orange/yellow color
PLEX_COLOR = (229, 160, 13)  # #e5a00d
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)

def create_standard_icon(size, filename):
    """Create a standard square icon with Plex branding."""
    img = Image.new('RGB', (size, size), PLEX_COLOR)
    draw = ImageDraw.Draw(img)

    # Draw a simple "P" or text
    try:
        # Try to use a system font
        font_size = size // 3
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
    except:
        font = ImageFont.load_default()

    text = "P"
    # Get text bounding box
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]

    # Center the text
    x = (size - text_width) // 2
    y = (size - text_height) // 2

    draw.text((x, y), text, fill=WHITE, font=font)

    img.save(filename, 'PNG')
    print(f"Created {filename}")

def create_maskable_icon(size, filename):
    """
    Create a maskable icon for Android adaptive icons.
    Maskable icons need safe zone (80% of canvas) for the important content.
    The outer 20% can be masked into different shapes (circle, squircle, etc).
    """
    img = Image.new('RGB', (size, size), PLEX_COLOR)
    draw = ImageDraw.Draw(img)

    # Safe zone is 80% of the icon (centered)
    safe_zone = int(size * 0.8)
    margin = (size - safe_zone) // 2

    # Draw content within safe zone
    try:
        font_size = safe_zone // 3
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
    except:
        font = ImageFont.load_default()

    text = "P"
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]

    # Center within safe zone
    x = (size - text_width) // 2
    y = (size - text_height) // 2

    draw.text((x, y), text, fill=WHITE, font=font)

    # Optional: Draw safe zone guide (comment out for production)
    # draw.rectangle([margin, margin, size-margin, size-margin], outline=WHITE, width=2)

    img.save(filename, 'PNG')
    print(f"Created {filename}")

def create_monochrome_icon(size, filename):
    """
    Create a monochrome icon for Android themed icons.
    These are single-color icons that Android can tint to match the user's theme.
    """
    # Create RGBA image with transparency
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Safe zone for maskable
    safe_zone = int(size * 0.8)

    try:
        font_size = safe_zone // 3
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
    except:
        font = ImageFont.load_default()

    text = "P"
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]

    x = (size - text_width) // 2
    y = (size - text_height) // 2

    # Draw in white (will be tinted by system)
    draw.text((x, y), text, fill=WHITE, font=font)

    img.save(filename, 'PNG')
    print(f"Created {filename}")

def main():
    icons_dir = "notifier_app/static/icons"
    os.makedirs(icons_dir, exist_ok=True)

    # Standard icon sizes
    standard_sizes = [72, 96, 128, 144, 152, 192, 384, 512]
    for size in standard_sizes:
        create_standard_icon(size, f"{icons_dir}/icon-{size}x{size}.png")

    # Maskable icon sizes (for Android adaptive icons)
    maskable_sizes = [192, 384, 512]
    for size in maskable_sizes:
        create_maskable_icon(size, f"{icons_dir}/icon-maskable-{size}x{size}.png")

    # Monochrome icon sizes (for Android themed icons)
    monochrome_sizes = [192, 512]
    for size in monochrome_sizes:
        create_monochrome_icon(size, f"{icons_dir}/icon-monochrome-{size}x{size}.png")

    print("\n✓ All PWA icons generated successfully!")
    print("\nIcon types created:")
    print("  • Standard icons: Work on all platforms (iOS, Android)")
    print("  • Maskable icons: Adaptive icons for Android (supports circle, rounded square, etc)")
    print("  • Monochrome icons: Themed icons for Android (adapts to user's color scheme)")
    print("\nTo customize: Edit this script or replace the generated PNG files with your design.")

if __name__ == "__main__":
    main()
