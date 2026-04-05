import cv2
import numpy as np

# Input and output paths
input_path = "/Users/MyMacBook/Desktop/Antigravity/Perkfinity/App-Perkfinity/App-Screenshots/Recording-Signup-1.mov"
output_path = "/Users/MyMacBook/Desktop/Antigravity/Perkfinity/App-Perkfinity/App-Screenshots/Recording-Signup-1-Edited.mp4"

cap = cv2.VideoCapture(input_path)

if not cap.isOpened():
    print("Error opening video file")
    exit()

fps = cap.get(cv2.CAP_PROP_FPS)
width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

# We want to slightly speed up the video so it fits under 30 seconds
# Current duration is ~29.8s. Let's speed it up by 5% to ensure safety.
target_fps = fps * 1.05

fourcc = cv2.VideoWriter_fourcc(*'mp4v')
out = cv2.VideoWriter(output_path, fourcc, target_fps, (width, height))

start_frame = int(7.0 * fps)
end_frame = int(18.0 * fps)

# The password strip is typically right above the standard iOS keyboard.
# Based on the screenshot provided, the keyboard strip is approximately
# between y-coordinates around 0.55 * height and 0.65 * height,
# but to be precise, let's blur/mask the exact region.
# Looking at the image, it's the iOS Safari/Password manager suggestion bar.
# It starts just above the keyboard buttons.
# Let's define the mask region based on percentages of height since we don't have exact pixel coords.
# In the provided image, the suggestion bar takes up the space from just below the done/chevron strip
# down to the 'q w e r t y' row.
# Let's say top = 58% of height, bottom = 68% of height.

y1 = int(height * 0.59)
y2 = int(height * 0.69)
x1 = 0
x2 = width

# Silver color in BGR is approximately (192, 192, 192). 
# We'll use a solid silver block.

frame_count = 0
while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break
        
    if start_frame <= frame_count <= end_frame:
        # Instead of just blurring, we can drop a solid silver rectangle
        # matching the keyboard background color (roughly BGR: 180, 180, 180 or 192, 192, 192)
        # Let's use BGR (198, 198, 198) based on typical iOS light gray.
        cv2.rectangle(frame, (x1, y1), (x2, y2), (209, 211, 217), -1)

    out.write(frame)
    frame_count += 1

cap.release()
out.release()

print(f"Successfully processed video. Saved to {output_path}")
