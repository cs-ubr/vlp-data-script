import os

"""
VLP fulltext parser for Invenio/Elasticsearch.
"""


class Fulltext:
    """
    This class parses VLP OCR files.
    """

    @staticmethod
    def parse_ocr_files_for_id(id):
        path = '../vlp/' + id
        ocr_files = []

        # r=root, d=directories, f = files
        if os.path.exists(path):
            for r, d, f in os.walk(path):
                for file in f:
                    page = {}
                    if '.txt' in file:
                        page['page_id'] = file
                        file = os.path.join(r, file)

                    with open(file, 'r') as ocr_file:
                        page['fulltext'] = ocr_file.read()
                       # print(ocr_file.read())

                    ocr_files.append(page)
        print(ocr_files)
        return ocr_files

if __name__ == "__main__":
    data = Fulltext.parse_ocr_files_for_id('lit9497')
    print(data)
    data = Fulltext.parse_ocr_files_for_id('lit9676')
    print(data)
    data = Fulltext.parse_ocr_files_for_id('TEST')
    print(data)
