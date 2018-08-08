def extract_orf(body):
    full_text = ""
    for text in body.xpath('//div[@id="ss-storyText"]/*/text()').extract():
        if len(text) > 100:
            full_text = full_text + "\n" + text
    return full_text[1:]


def extract_kurier(body):
    full_text = ""
    for text in body.xpath('//div[@class="article-paragraph"]/*//text()').extract():
        full_text = full_text + " " + text.strip()
    return full_text[1:]
