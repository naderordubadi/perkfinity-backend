import glob
from PIL import Image, ImageDraw, ImageFont

input_dir = "/Users/MyMacBook/Desktop/Antigravity/Perkfinity/App-Perkfinity/App-Screenshots"
files = glob.glob(input_dir + "/Perkfinity-*.PNG")

target_size = (1284, 2778)
bg_color = (255, 255, 255)  # white

try:
    font = ImageFont.truetype("/System/Library/Fonts/HelveticaNeue.ttc", 76, index=1)
    sub_font = ImageFont.truetype("/System/Library/Fonts/HelveticaNeue.ttc", 48, index=0)
except Exception:
    font = ImageFont.load_default()
    sub_font = ImageFont.load_default()

copy_map = {
    "Perkfinity-1": ("Absolute Privacy Guaranteed.", "Stores never see your personal data."),
    "Perkfinity-2": ("Instant Checkout Perks.", "Zero friction. No plastic cards needed."),
    "Perkfinity-3": ("One App. Every Shop.", "Ditch the endless individual store apps."),
    "Perkfinity-4": ("Free to Join.", "Sign up in seconds. Get perks for infinity."),
    "Perkfinity-5": ("Secure Account Access.", "Bank-level security for your peace of mind."),
    "Perkfinity-6": ("Flash Sales Nearby.", "Get alerted the moment a local deal drops."),
    "Perkfinity-7": ("Discover Local Favorites.", "Exclusive perks at shops in your neighborhood."),
    "Perkfinity-8": ("Claim Exclusive Perks.", "Unlock massive discounts with a single tap."),
    "Perkfinity-9": ("Lock In Your Savings.", "Activate dynamic offers right before you order."),
    "Perkfinity-10": ("Redeem at the Register.", "Just show the cashier. It's that simple."),
    "Perkfinity-11": ("Watch Your Savings Grow.", "Track every dollar saved across all stores."),
    "Perkfinity-12": ("Never Miss a Drop.", "Get VIP alerts for new perks in your area."),
    "Perkfinity-13": ("Scan to Unlock.", "Activate the perk that store created specifically for you.")
}

files_to_process = [f for f in files if "-cornered" not in f and "-appstore" not in f]

for input_path in files_to_process:
    base_name = input_path.split("/")[-1].replace(".PNG", "")
    output_path = f"{input_dir}/{base_name}-appstore.PNG"
    
    canvas = Image.new("RGBA", target_size, bg_color)
    draw = ImageDraw.Draw(canvas)

    title, subtitle = copy_map.get(base_name, ("Privacy-First Perks.", "Earn local rewards instantly."))

    draw.text((642, 120), title, font=font, fill=(30, 30, 30), anchor="ms")
    draw.text((642, 210), subtitle, font=sub_font, fill=(100, 100, 100), anchor="ms")

    screenshot = Image.open(input_path).convert("RGBA")

    frame_width = 1110
    aspect = screenshot.height / screenshot.width
    frame_height = int(frame_width * aspect)
    screenshot = screenshot.resize((frame_width, frame_height), Image.Resampling.LANCZOS)

    rad = 120
    mask = Image.new("L", (frame_width, frame_height), 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.rounded_rectangle((0, 0, frame_width, frame_height), radius=rad, fill=255)

    bezel_thickness = 24
    bezel_width = frame_width + bezel_thickness * 2
    bezel_height = frame_height + bezel_thickness * 2
    bezel_mask = Image.new("RGBA", (bezel_width, bezel_height), (0,0,0,0))
    bezel_draw = ImageDraw.Draw(bezel_mask)

    bezel_draw.rounded_rectangle((0, 0, bezel_width, bezel_height), radius=rad + bezel_thickness - 8, fill=(30, 30, 35, 255))
    bezel_draw.rounded_rectangle((bezel_thickness - 4, bezel_thickness - 4, bezel_width - bezel_thickness + 4, bezel_height - bezel_thickness + 4), radius=rad+4, outline=(70, 70, 80, 255), width=4)

    notch_w = 460
    notch_h = 80
    notch_x = (bezel_width - notch_w) // 2
    bezel_draw.rounded_rectangle((notch_x, bezel_thickness, notch_x + notch_w, bezel_thickness + notch_h), radius=40, fill=(30, 30, 35, 255))

    x_offset = (target_size[0] - bezel_width) // 2
    y_offset = target_size[1] - bezel_height - 60

    canvas.alpha_composite(bezel_mask, (x_offset, y_offset))
    canvas.paste(screenshot, (x_offset + bezel_thickness, y_offset + bezel_thickness), mask)

    canvas = canvas.convert("RGB")
    canvas.save(output_path)
    print(f"Saved {output_path}")

print("All screenshots processed successfully.")
