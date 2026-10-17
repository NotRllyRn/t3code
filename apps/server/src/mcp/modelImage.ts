import sharp from "sharp";

const MAX_MODEL_IMAGE_BYTES = 500 * 1024;
const MODEL_IMAGE_ATTEMPTS = [
  { size: 1280, quality: 80 },
  { size: 1280, quality: 65 },
  { size: 1280, quality: 50 },
  { size: 960, quality: 50 },
  { size: 720, quality: 45 },
  { size: 480, quality: 45 },
  { size: 320, quality: 45 },
] as const;

export interface ModelImage {
  readonly data: Uint8Array;
  readonly mimeType: "image/jpeg" | "image/png";
}

export async function modelImage(
  png: Uint8Array,
  detail: "model" | "original" = "model",
): Promise<ModelImage> {
  if (detail === "original") return { data: png, mimeType: "image/png" };

  let data = png;
  for (const attempt of MODEL_IMAGE_ATTEMPTS) {
    data = await sharp(png)
      .rotate()
      .resize(attempt.size, attempt.size, { fit: "inside", withoutEnlargement: true })
      .flatten({ background: "#fff" })
      .jpeg({ quality: attempt.quality, mozjpeg: true })
      .toBuffer();
    if (data.byteLength <= MAX_MODEL_IMAGE_BYTES) break;
  }
  if (data.byteLength > MAX_MODEL_IMAGE_BYTES) throw new Error("Could not bound model image");
  return { data, mimeType: "image/jpeg" };
}
