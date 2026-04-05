import glob
from PIL import Image

output_dir = "/Users/MyMacBook/Desktop/Antigravity/Perkfinity/App-Perkfinity/App-Screenshots"
files = glob.glob(output_dir + "/*-appstore.PNG")

print("Checking Apple App Store Requirements for 6.5-inch devices...")
print("Requirement: 1284 x 2778 pixels, No Alpha Channel (RGB), PNG format.\n")

all_passed = True

for f in files:
    try:
        with Image.open(f) as img:
            file_name = f.split('/')[-1]
            width, height = img.size
            mode = img.mode
            format = img.format
            
            passed_size = (width == 1284 and height == 2778)
            passed_mode = (mode == 'RGB')
            passed_format = (format == 'PNG')
            
            status = "PASS" if (passed_size and passed_mode and passed_format) else "FAIL"
            if status == "FAIL":
                all_passed = False
                
            print(f"[{status}] {file_name}: {width}x{height}, Mode: {mode}, Format: {format}")
    except Exception as e:
        print(f"[ERROR] Could not process {f}: {e}")
        all_passed = False

if all_passed:
    print("\n✅ Verification Complete: ALL images meet strict Apple formatting requirements.")
else:
    print("\n❌ Verification Failed: One or more images violated the requirements.")
