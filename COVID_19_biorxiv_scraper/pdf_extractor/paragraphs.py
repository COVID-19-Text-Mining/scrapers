import re
import string
import sys
from collections import Counter
from io import StringIO

from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams, LTContainer, LTTextBox, LTLayoutContainer, LTTextLineHorizontal, \
    LTTextBoxHorizontal, LTTextBoxVertical
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from pdfminer.utils import Plane, uniq


def group_textlines(self, laparams, lines):
    """Patched class method that fixes empty line aggregation, and allows
    run-time line margin detection"""
    plane = Plane(self.bbox)
    plane.extend(lines)
    boxes = {}
    for line in lines:
        neighbors = line.find_neighbors(plane, laparams.line_margin)
        if line not in neighbors or not line.get_text().strip():
            continue

        # Correct margin to paragraph specific
        true_margin = laparams.line_margin
        for obj1 in neighbors:
            if obj1 is line:
                continue
            margin = min(abs(obj1.y0 - line.y1), abs(obj1.y1 - line.y0))
            margin = margin * 1.05 / line.height
            if margin < true_margin:
                true_margin = margin

        neighbors = line.find_neighbors(plane, true_margin)
        if line not in neighbors:
            continue

        members = []
        for obj1 in neighbors:
            if not obj1.get_text().strip():
                continue
            members.append(obj1)
            if obj1 in boxes:
                members.extend(boxes.pop(obj1))
        if isinstance(line, LTTextLineHorizontal):
            box = LTTextBoxHorizontal()
        else:
            box = LTTextBoxVertical()
        for obj in uniq(members):
            box.add(obj)
            boxes[obj] = box
    done = set()
    for line in lines:
        if line not in boxes:
            continue
        box = boxes[line]
        if box in done:
            continue
        done.add(box)
        if not box.is_empty():
            yield box
    return


# Patch the method
LTLayoutContainer.group_textlines = group_textlines


class TextHandler(TextConverter):
    def __init__(self, rsrcmgr, laparams=None):
        _laparams = {
            'char_margin': 3.0,
            'line_margin': 2.5
        }
        _laparams.update(laparams or {})
        super(TextHandler, self).__init__(
            rsrcmgr, StringIO(), laparams=LAParams(**_laparams))

        self.pages = []

    def receive_layout(self, ltpage):
        paragraphs = []

        def render(item):
            if isinstance(item, LTTextBox):
                paragraphs.append({
                    'text': item.get_text(),
                    'bbox': item.bbox
                })

            if isinstance(item, LTContainer):
                for child in item:
                    render(child)

        render(ltpage)
        self.pages.append(paragraphs)

    def get_true_paragraphs(self):
        # Drop redundant paragraphs
        counter_by_page = Counter()
        for page in self.pages:
            for item in page:
                counter_by_page[item['text']] += 1
        redundant = set(x for x, y in counter_by_page.items() if y > 1)
        for page in self.pages:
            page[:] = list(filter(
                lambda x: x['text'] not in redundant,
                page
            ))

        # Drop number only paragraphs
        printable = set(string.printable)
        for page in self.pages:
            new_page = []
            for item in page:
                normalized_text = ''.join((filter(lambda k: k in printable, item['text']))).strip()
                words = re.findall(r'[a-zA-Z]', item['text'])
                if len(words) > 0.5 * len(normalized_text):
                    new_page.append(item)
            page[:] = new_page

        # Convert newlines and excessive whitespaces
        for page in self.pages:
            for word in page:
                word['text'] = re.sub(r'\s+', ' ', word['text'])

        return self.pages


def extract_paragraphs_pdf(pdf_file, return_dicts=False, only_printable=True, laparams=None):
    """
    pdf_file is a file-like object.
    This function will return lists of plain-text paragraphs."""
    parser = PDFParser(pdf_file)
    doc = PDFDocument(parser)
    rsrcmgr = PDFResourceManager()
    device = TextHandler(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    for page in PDFPage.create_pages(doc):
        interpreter.process_page(page)

    def paragraph_pos_rank(p):
        x, y = p['bbox'][0], -p['bbox'][1]
        return int(y)

    def is_ending_char(c):
        return re.match(r'!\.\?', c) is not None

    paragraphs = []

    printable = set(string.printable)
    for page_num, page in enumerate(device.get_true_paragraphs()):
        for j, p in enumerate(sorted(page, key=paragraph_pos_rank)):
            text = p['text']

            if only_printable:
                text = ''.join(filter(lambda x: x in printable, text))

            text = text.strip()

            indention_level = int(p['bbox'][0] / 10)

            if j == 0 and len(paragraphs) > 0:
                last_paragraph = paragraphs[-1]
                if return_dicts:
                    last_paragraph = last_paragraph['text']
                should_join = (not is_ending_char(last_paragraph[-1]) and
                               not text[0].isupper())

                if should_join:
                    text = last_paragraph + ' ' + text
                    if return_dicts:
                        indention_level = paragraphs[-1]['indention_level']
                    del paragraphs[-1]

            if return_dicts:
                paragraphs.append({
                    'text': text,
                    'page_num': page_num,
                    'indention_level': indention_level,
                    'bbox': p['bbox']
                })
            else:
                paragraphs.append(text)

    return paragraphs


if __name__ == '__main__':
    if len(sys.argv) == 3:
        _, input_file, output_file = sys.argv
        input_file = open(input_file, 'rb')
        output_file = open(output_file, 'w')
    elif len(sys.argv) == 2:
        _, input_file = sys.argv
        input_file = open(input_file, 'rb')
        output_file = sys.stdout
    else:
        print('Usage: %s <input_pdf> [output_txt]' % sys.argv[0])
        sys.exit(1)

    for i, paragraph in enumerate(extract_paragraphs_pdf(input_file)):
        output_file.write('------ Paragraph %d ------\n\n' % i)
        output_file.write(paragraph)
        output_file.write('\n\n')
