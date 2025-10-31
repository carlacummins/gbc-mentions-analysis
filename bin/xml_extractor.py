from pathlib import Path
import sys

from gbcutils.europepmc import _extract_article_from_combined_xml

def extract_article_python(pmcid: str, big_xml: str, outdir: str = ".") -> Path:
    """Extract the article with given PMCID from a big XML file."""
    target_tag = f">{pmcid}<"
    output_path = Path(outdir) / f"PMC{pmcid}.xml"

    inside_article = False
    write_article = False

    with open(big_xml, "r", encoding="utf-8") as infile, open(output_path, "w", encoding="utf-8") as outfile:
        for line in infile:
            # Detect start of <article>
            if "<article " in line:
                inside_article = True
                buffer = [line]  # buffer the first line of the article
                write_article = False
                continue

            if inside_article:
                buffer.append(line)

                # Check if this article matches our pmcid
                if target_tag in line and not write_article:
                    write_article = True

                # End of article → write if matched and exit
                if "</article>" in line:
                    if write_article:
                        outfile.writelines(buffer)
                        return output_path
                    inside_article = False  # reset for next article

    raise ValueError(f"PMCID {pmcid} not found in {big_xml}")

# extract_article_python(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else ".")
article = _extract_article_from_combined_xml(sys.argv[1], sys.argv[2])
print(article)