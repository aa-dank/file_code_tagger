# extracting/image_extractor.py
import logging
import pytesseract
import re
from typing import List
from pathlib import Path
from PIL import Image, ImageOps, ImageSequence
import numpy as np

logger = logging.getLogger(__name__)

try:
    import cv2  # optional for better preprocessing
    _HAS_CV2 = True
except ImportError:
    _HAS_CV2 = False

from .basic_extraction import FileTextExtractor


class ImageTextExtractor(FileTextExtractor):
    """
    OCR text from image files using Tesseract (via pytesseract).

    Supports automatic orientation correction via Tesseract OSD,
    plus optional light pre-processing for better OCR on scans/phone pics.
    
    Supports: PNG, JPG/JPEG, TIFF, BMP, GIF (first frame), HEIC (if pillow-heif installed).
    """
    file_extensions: List[str] = ["png", "jpg", "jpeg", "tif", "tiff", "bmp", "gif"]

    def __init__(self,
                 lang: str = "eng",
                 tesseract_cmd: str | None = None,
                 psm: int = 3,
                 oem: int = 3,
                 preprocess: bool = True,
                 max_side: int = 3000):
        """
        Parameters
        ----------
        lang : str
            Tesseract language(s). e.g. "eng+spa".
        tesseract_cmd : str | None
            Full path to tesseract.exe if not on PATH. (eg r"C:\Program Files\Tesseract-OCR\tesseract.exe")
        psm : int
            Page segmentation mode. 3 = fully automatic, 6 = assume uniform blocks of text.
        oem : int
            OCR Engine mode. 3 = default, based on what is available.
        preprocess : bool
            Whether to apply grayscale/threshold/denoise pre-processing.
        max_side : int
            Resize largest image side to this (keeps memory reasonable).
        """
        super().__init__()
        if tesseract_cmd:
            pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
        self.lang = lang
        self.psm = psm
        self.oem = oem
        self.preprocess = preprocess
        self.max_side = max_side

    def __call__(self, path: str) -> str:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(path)

        images = self._load_images(p)
        texts = []
        for img in images:
            # detect and correct orientation
            img = self.detect_and_correct_orientation(img)
            if self.preprocess:
                img = self._preprocess(img)
            cfg = f"--psm {self.psm} --oem {self.oem}"
            txt = pytesseract.image_to_string(img, lang=self.lang, config=config_str(cfg))
            texts.append(txt)

        return "\n".join(texts)

    # ---------- helpers ----------
    def _load_images(self, path: Path) -> List[Image.Image]:
        """Handle multi-page TIFFs and GIFs gracefully."""
        imgs = []
        with Image.open(path) as im:
            try:
                for frame in ImageSequence(im):
                    imgs.append(frame.convert("RGB"))
            except Exception:
                # Not multi-frame
                imgs.append(im.convert("RGB"))
        # Resize if gigantic
        out = []
        for img in imgs:
            if max(img.size) > self.max_side:
                scale = self.max_side / max(img.size)
                new_sz = (int(img.width * scale), int(img.height * scale))
                img = img.resize(new_sz, Image.LANCZOS)
            out.append(img)
        return out

    def _preprocess(self, pil_img: Image.Image) -> Image.Image:
        """
        Simple preprocessing:
          - convert to grayscale
          - optional OpenCV adaptive threshold / denoise if available
        """
        if _HAS_CV2:
            img = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2GRAY)
            # adaptive threshold helps on uneven lighting
            img = cv2.adaptiveThreshold(img, 255,
                                        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                        cv2.THRESH_BINARY, 31, 10)
            return Image.fromarray(img)
        else:
            # Pillow-only fallback
            img = ImageOps.grayscale(pil_img)
            # Simple point threshold
            img = img.point(lambda x: 255 if x > 200 else 0)
            return img

    def detect_and_correct_orientation(self, pil_img: Image.Image) -> Image.Image:
        """
        Use Tesseract OSD to detect rotation and counter-rotate image upright.
        """
        osd = pytesseract.image_to_osd(pil_img)
        rot_match = re.search(r"Rotate: (\d+)", osd)
        if rot_match:
            angle = int(rot_match.group(1))
            if angle != 0:
                pil_img = pil_img.rotate(360 - angle, expand=True)
        return pil_img


# Small utility so we can extend config easily
def config_str(*parts: str) -> str:
    return " ".join(part for part in parts if part)
