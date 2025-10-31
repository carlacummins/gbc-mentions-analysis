pmcid=$1
big_xml=$2

line=$(grep -n "<article-id[^>]*pmcid[^>]*>$pmcid<" ${big_xml} | cut -d: -f1)
start=$(awk -v line=$line 'NR<=line && /<article /{s=NR} END{print s}' ${big_xml})
end=$(awk -v line=$line 'NR>=line && /<\/article>/{print NR; exit}' ${big_xml})
sed -n "${start},${end}p" ${big_xml} > PMC${pmcid}.xml
