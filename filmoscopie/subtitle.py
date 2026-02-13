"""
https://github.com/glut23/webvtt-py
https://huggingface.co/blog/sentiment-analysis-python
"""

from transformers import pipeline
import webvtt


def vtt_to_text(path: str):
    for caption in webvtt.read(path):
        yield caption.text


sentiment_pipeline = pipeline("sentiment-analysis")


def sentiment(texts: list[str]):
    s: list[dict[str, float]] = sentiment_pipeline(texts)
    score = dict(NEGATIVE=0, POSITIVE=0)
    for txt, sent in zip(texts, s):
        if txt.startswith("["):
            continue
        print(txt)
        print(f"\t{sent['label']} {sent['score']:.2f}")
        score[sent["label"]] += sent["score"]
    print(score)


if __name__ == "__main__":
    import sys

    sentiment(list(vtt_to_text(sys.argv[1])))
