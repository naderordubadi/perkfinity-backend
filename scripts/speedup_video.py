import cv2
import numpy as np

# Input and output paths
input_path = "/Users/MyMacBook/Desktop/Antigravity/Perkfinity/App-Perkfinity/App-Screenshots/Recording-Signup-2.mov"
output_path = "/Users/MyMacBook/Desktop/Antigravity/Perkfinity/App-Perkfinity/App-Screenshots/Recording-Signup-2-Edited.mp4"

cap = cv2.VideoCapture(input_path)

if not cap.isOpened():
    print("Error opening video file")
    exit()

fps = cap.get(cv2.CAP_PROP_FPS)
width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

# Original duration is ~32.85 seconds.
# We need it under 30.0s safely, let's say 28.5s.
# Speed factor = 32.85 / 28.5 = ~1.15 (15% faster).
target_fps = fps * 1.15

fourcc = cv2.VideoWriter_fourcc(*'mp4v')
out = cv2.VideoWriter(output_path, fourcc, target_fps, (width, height))

frame_count = 0
while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    out.write(frame)
    frame_count += 1
    
    if frame_count % 300 == 0:
        print(f"Processed {frame_count} / {total_frames} frames...")

cap.release()
out.release()

print(f"Successfully processed video. Saved to {output_path}")
