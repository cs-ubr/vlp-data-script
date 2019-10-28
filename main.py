# -*- coding: utf-8 -*-
import sys
import argparse
from parser import Parser
import config as cfg

_author_ = "Colin Sippl"
_organization_ = "University Library of Regensburg"
_email_ = "colin.sippl@ur.de"
_license_ = "GPL 3"


def main():
    parser = argparse.ArgumentParser(description='Parsing VLP data.')
    parser.add_argument('files', metavar='f', type=str, nargs='+',
                        help='file name accumulator')
    parser.add_argument('-p', dest='accumulate', action='store_const',
                        const=sum, default=max,
                        help='parse specific VLP file')

    args = parser.parse_args()
    # iterieren?
    parse_vlp_file(args.files[0])
    # print(args.accumulate(args.integers))


def parse_vlp_file(filename):
    if (filename == 'vlp'):
        Parser.parse_file('data/vlp.csv')

    if (filename == 'VL-People2'):
        Parser.create_people(True)
        Parser.parse_people_file('data/VL-People2.csv')

    if (filename == 'sites'):
        vlp_sites = Parser.create_sites()
        Parser.create_json_file(vlp_sites, 'vlp_sites')

    if (filename == 'cli'):

        # get all vlp sites
        vlp_sites = Parser.create_sites()

        # get all essays
        essays = Parser.create_records_db('SELECT * FROM vl_essays', False)

        # get all journal super collections
        journal_collections = Parser.create_records_db("SELECT * FROM vl_literature where referencetype='(Journal)'", False)

        # get all journal volumes with refs
        journal_volumes = Parser.create_records_db("SELECT * FROM vl_literature where referencetype='(JournalVolume)' and volumeid is not null", False)

        # get all journal articles with refs
        #journal_articles = Parser.create_records_db("SELECT * FROM vl_literature where referencetype='Journal Article' and volumeid is not null LIMIT " + str(cfg.ROW_LIMIT), False)
        journal_articles = Parser.create_records_db(
            "SELECT * FROM vl_literature where referencetype='Journal Article' and volumeid is not null and author is not null LIMIT " + str(
                cfg.ROW_LIMIT), False)

        # (Book) or (Edited Book) super collections
        book_collections = Parser.create_records_db("SELECT * FROM vl_literature where (referencetype='(Book)' or referencetype='(Edited Book)') and volumeid is null", False)

        # 'Book Section' or 'Book' super collections
        #book_collections2 = Parser.create_records_db("SELECT * FROM vl_literature where (referencetype='Book Section' or referencetype='Book') and volumeid is null and id < 5000", False)
        book_collections2 = Parser.create_records_db(
            "SELECT * FROM vl_literature where (referencetype='Book Section' or referencetype='Book') and volumeid is null",
            False)

        # get all book sub entries
        #books = Parser.create_records_db("SELECT * FROM vl_literature where (referencetype='Book Section' or referencetype='Book' or referencetype='(Book)') and volumeid is not null and id < 5000", False)
        books = Parser.create_records_db(
            "SELECT * FROM vl_literature where (referencetype='Book Section' or referencetype='Book' or referencetype='(Book)') and volumeid is not null",
            False)

        # get all people
        vlp_people =  Parser.create_people("SELECT * FROM vl_people")

        # create JSON files
        Parser.create_json_file(vlp_sites, 'vlp_sites')
        Parser.create_json_file(essays, 'essays')
        Parser.create_json_file(vlp_people, 'vlp_people')
        Parser.create_json_file(journal_collections, 'journal_collections')
        Parser.create_json_file(journal_volumes, 'journal_volumes')
        Parser.create_json_file(journal_articles, 'journal_articles')
        Parser.create_json_file(book_collections, 'book_collections')
        Parser.create_json_file(book_collections2, 'book_collections2')
        Parser.create_json_file(books, 'books')

        
        #records = Parser.create_records_db(Parser.query_a)
        #Parser.post_records_via_cli(records)
        #Parser.do_reindex_via_cli()



if __name__ == "__main__":
    main()