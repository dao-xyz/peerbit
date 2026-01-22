import { resolveRelativePath } from "../utils/path";

function isExternal(src: string) {
	return /^https?:\/\//.test(src);
}

export function MarkdownImage({
	base,
	markdownDir,
	src,
	alt,
}: {
	base: string;
	markdownDir: string;
	src: string;
	alt: string;
}) {
	if (!src || isExternal(src) || src.startsWith("/")) {
		return <img src={src} alt={alt} />;
	}

	const resolved = resolveRelativePath(markdownDir, src);
	return <img src={`/${base}/${resolved}`} alt={alt} />;
}
