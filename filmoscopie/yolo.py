"""
https://docs.ultralytics.com/models/yoloe/#fine-tuning-on-custom-dataset
"""

from ultralytics import YOLO
import sys

# Initialize model
model = YOLO("yoloe-26l-seg-pf.pt")

# Run prediction. No prompts required.
results = model.predict(sys.argv[1])
# print(results)
# Show results
results[0].show()
