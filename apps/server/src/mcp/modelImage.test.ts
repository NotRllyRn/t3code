import { expect, it } from "@effect/vitest";
import sharp from "sharp";
import { modelImage } from "./modelImage.ts";

it("bounds model-facing images without changing originals", async () => {
  const pixels = Uint8Array.from(
    { length: 1600 * 1400 * 3 },
    (_, index) => (index * 31 + (index >> 7)) & 0xff,
  );
  const png = await sharp(pixels, {
    raw: { width: 1600, height: 1400, channels: 3 },
  })
    .png()
    .toBuffer();

  const bounded = await modelImage(png);
  const metadata = await sharp(bounded.data).metadata();
  expect(bounded.mimeType).toBe("image/jpeg");
  expect(bounded.data.byteLength).toBeLessThanOrEqual(500 * 1024);
  expect(Math.max(metadata.width ?? 0, metadata.height ?? 0)).toBeLessThanOrEqual(1280);

  const original = await modelImage(png, "original");
  expect(original).toEqual({ data: png, mimeType: "image/png" });
});
