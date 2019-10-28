# -*- coding: utf-8 -*-
import csv
import requests
import config as cfg
from requests.packages.urllib3.exceptions import InsecureRequestWarning
#import subprocess
#import sys
import os
#from requests.auth import HTTPBasicAuth
import json
import re
from dbconn import Dbconn
#from fulltext import Fulltext

_author_ = "Colin Sippl"
_organization_ = "University Library of Regensburg"
_email_ = "colin.sippl@ur.de"
_license_ = "GPL 3"

"""
VLP data parser for Invenio.
"""


class Parser:
    """
    This class parses CSV files.
    """

    INVENIO_SERVICE_URL = cfg.INVENIO_SERVICE_URL
    ROW_LIMIT = cfg.ROW_LIMIT
    REQUESTS_SSL_VERIFY = cfg.REQUESTS_SSL_VERIFY
    query_a = 'SELECT * FROM vl_literature and id >= 1 and id <=' + \
        str(ROW_LIMIT)
    #query_a = "SELECT * FROM vl_literature where volumeid is not null or startpages is not null LIMIT " + str(ROW_LIMIT)
    query_b = "SELECT * FROM vl_literature where volumeid is null and startpages is null and referencetype != 'Journal Article' LIMIT " + \
        str(ROW_LIMIT)

    # hide InsecureRequestWarning
    # https://stackoverflow.com/questions/27981545/suppress-insecurerequestwarning-unverified-https-request-is-being-made-in-pytho
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    @staticmethod
    def parse_file(path):
        print(path)
        # https://docs.python.org/2/library/csv.html
        #response_array = []

        with open(path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            number_rows = 0
            for row in reader:
                if number_rows < Parser.ROW_LIMIT:
                    record = Parser.create_records(row, True)
                    # response_array.append(record)
                    # Parser.create_people(row)
                    print(
                        "==========================================================================")
                    print("Zeile " + str(number_rows))
                    number_rows += 1


    # generate people entries from vl-people2.csv
    @staticmethod
    def parse_people_file(path):
        print(path)
        # https://docs.python.org/2/library/csv.html
        with open(path, 'rU') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
        #reader = csv.reader(open(path, 'rU'), dialect=csv.excel_tab, delimiter=';')
            number_rows = 0
            for row in reader:
                if number_rows < Parser.ROW_LIMIT:
                    # print(row)
                    Parser.create_people(row)
                    number_rows += 1

    @staticmethod
    def create_records_db(query, do_lines):
        dbconn = Dbconn()
        dbconn.connect()
        # reference, id, referencetype, author, title, "year", "date", volume,
        # secondarytitle, startpages, pages, sql_year, sql_date, fullreference,
        # shortreference, liseurl, lastupdate, volumeid, authorized, online, cd,
        # project, authorrefdisplay, yearrefdisplay, titlerefdisplay, detailsrefdisplay,
        # related, endnoteexport, volumeid_search, notes, transcription, url

        # id, author, title, "year", "date", volume,
        # secondarytitle, startpages, pages, sql_year, sql_date, fullreference,
        # shortreference, liseurl, lastupdate, volumeid, authorized, online, cd,
        # project, authorrefdisplay, yearrefdisplay, titlerefdisplay, detailsrefdisplay,
        # related, endnoteexport, volumeid_search, notes, transcription, url
        dbconn.execute_st(query)
        result = dbconn.fetch_result()
        dbconn.disconnect()
        records = []
        for data in result:

            json_data = {}
            json_data['identifier'] = data['reference']
            cref_data = {}
            cref = Parser.get_related_authors(data['reference'])
            if cref is not None:
                cref_data['$ref'] = "https://vlp.ur.de/api/resolver/author/" + cref
                json_data['authid'] = cref
                json_data['creator'] = cref_data
            json_data['title'] = data['title']
            if 'secondarytitle' in data:
                json_data['alternative'] = data['secondarytitle']
            #json_data['keywords'] = [data['referencetype']]
            json_data['type'] = data['referencetype']
            if 'year' in data:
                json_data['issued'] = data['year']
            #else:
            #    json_data['issued'] = data['created']
            #json_data['ocr'] = Fulltext.parse_ocr_files_for_id(data['reference'])

            if data['author'] is not None:
                #json_data['creator'] = data['author']
                #print(data[year'author'])
                json_data['contributors'] = Parser.get_authors(data['author'], cref, "author")
                #json_data['contributors'] = Parser.rebuild_refs_sites(data['author'], cref, "author")

            if 'volume' in data and data['volume'] is not None:
                json_data['volume'] = data['volume']

            if 'volume' in data and data['volume'] is not None:
                json_data['pages'] = data['pages']

            if data['referencetype'] == '(Journal)' or data['referencetype'] == '(JournalVolume)':
                json_data['title'] = data['secondarytitle']
            # else:
            # if  data['secondarytitle'] is not None and not Parser.is_empty(data['secondarytitle']):

            if 'detailsrefdisplay' in data and data['detailsrefdisplay'] is not None and not Parser.is_empty(data['detailsrefdisplay']):
                json_data['detailsRefDisplay'] = data['detailsrefdisplay']

            if 'volumeid' in data and data['volumeid'] is not None and not Parser.is_empty(data['volumeid']) and data['volumeid'] is not '':
                json_data['volume_id'] = data['volumeid']
                ref = {}
                ref['$ref'] = "https://vlp.ur.de/api/resolver/record/" + \
                    data['volumeid']
                json_data['record'] = ref

            if do_lines:
                Parser.post_record(json_data, 'records')
            else:
                records.append(json_data)

        return records

    @staticmethod
    def is_empty(field):
        return field.isspace()

    @staticmethod
    def get_related_authors(lit_reference):
        dbconn = Dbconn()
        dbconn.connect()
        query = "SELECT vl_people.reference FROM vl_literature, vl_people WHERE vl_people.fullname=vl_literature.author AND vl_literature.reference='" + lit_reference + "'"
        dbconn.execute_st(query)
        result = dbconn.fetch_result()
        dbconn.disconnect()
        if result is not None:
            for row in result:
                print(result)
                return row['reference']

    @staticmethod
    def create_people(query):
        dbconn = Dbconn()
        dbconn.connect()
        # id, created, created_by, modified_by, reference, marker, clip,
        # complete, "name", experiments, related, born, died, born_day, born_month,
        # born_year, died_day, died_month, died_year, first_name, career, degrees,
        # positions, technologies, main1, born_place, died_place, shortreference,
        # main2, main3, main4, main5, sources, image, comment_intern, literature,
        # sources2, fullname, lifespan4frontier, works, sourceslist, variationsname,
        # variationsfirstname, clip2, searchstring, lifespandisplay, marker2,
        # category, statistik, completed, customsearchstring, in_essay, bibliography,
        # imagesource, modified, article_autor, article_autor_url
        dbconn.execute_st(query)
        result = dbconn.fetch_result()
        dbconn.disconnect()
        people = []
        for row in result:
            people_data = {}
            #people_data['name'] = row['shortreference']
            # https://invenio-indexer.readthedocs.io/en/latest/usage.html#indexing-a-record
            people_data['$schema'] = 'http://vlp.ur.de/schemas/authors/author-v1.0.0.json'
            people_data['name'] = row['name']
            related_literature_ids = []
            if row['first_name'] is not None and row['first_name'] is not '':
                people_data['first_name'] = row['first_name']
            people_data['identifier'] = row['reference']
            if row['career'] is not None:
                people_data['career'] = Parser.rebuild_hyperlinks(row['career'], None)[0]
            if row['born_year'] is not None and row['died_year'] is not None:
                people_data['life_span'] = {"year_of_birth": int(
                    row['born_year']), "year_of_death": int(row['died_year'])}
            #life_span = Parser.get_life_span(row['shortReference'])
            #people_data['life_span'] = life_span
            if row['born_place'] is not None:
                people_data['born_place'] = row['born_place']
            if row['died_place'] is not None:
                people_data['died_place'] = row['died_place']
            if row['image'] is not None:
                people_data['image'] = row['image']
            if row['degrees'] is not None:
                people_data['degrees'] = row['degrees']

            if row['works'] is not None:
                people_data['references'] = Parser.rebuild_refs_sites(row['works'], None , None)

            #if do_lines:
            #    Parser.post_record(people_data, 'authors')
            #else:
            people.append(people_data)
            #Parser.post_record(people_data, 'authors')
            # people.append(people_data)

        for person in people:
            if 'references' in person:
                for reference in person['references']:
                    if 'identifier' in reference:
                        if reference['identifier'].startswith('lit'):
                            related_literature_ids.append(reference['identifier'])

        #print(related_literature_ids)
        people_literature = Parser.create_sites_literature(list(set(related_literature_ids)))
        Parser.create_json_file(people_literature, 'people_literature')

        return people

    @staticmethod
    def create_sites():
        #query = "SELECT * FROM public.vl_sites where reference='sit137'"
        query = 'SELECT * FROM vl_sites LIMIT ' + str(Parser.ROW_LIMIT)
        dbconn = Dbconn()
        dbconn.connect()
        dbconn.execute_st(query)
        result = dbconn.fetch_result()
        dbconn.disconnect()
        sites = []
        related_literature_ids = []
        for row in result:
            site = {}
            site['$schema'] = 'http://vlp.ur.de/schemas/sites/site-v1.0.0.json'
            site['identifier'] = row['reference']
            site['name'] = row['labname']

            if row['established'] is not None:
                site['date'] = row['established']
            if row['location'] is not None:
                site['location'] = row['location']

            keywordsb = Parser.get_labs_disciplines(row['labs_disciplines'])
            if len(keywordsb) > 0:
                site['keywordsb'] = keywordsb

            #site['keywordsb'] = [{'tag':'text'},{'tag':'toll'}]

            contributors = []
            contributor = {}
            if row['people_head'] is not None:
                contributors = contributors + Parser.rebuild_refs_sites(row['people_head'], None , "head")
            if row['people_architect'] is not None:
                contributors = contributors + Parser.rebuild_refs_sites(row['people_architect'], None, "architect")
            if row['people_deptheads'] is not None:
                contributors = contributors + Parser.rebuild_refs_sites(row['people_deptheads'], None, "depthead")
            if row['people_others'] is not None:
                contributors = contributors + Parser.rebuild_refs_sites(row['people_others'],None, "N/A")
            if row['people_assistants'] is not None:
                contributors = contributors + Parser.rebuild_refs_sites(row['people_assistants'],None, "assistant")
            
            if len(contributors) > 0:
                site['contributors'] = contributors
                #print(contributors)

            images = []
            if row['img1id'] is not None and row['img1id'] != '':
                images.append({"identifier" : row['img1id']}) 
            if row['img2id'] is not None and row['img2id'] != '':
                images.append({"identifier" : row['img2id']}) 
            if row['img3id'] is not None and row['img3id'] != '':
                images.append({"identifier" : row['img3id']}) 
            if row['img4id'] is not None and row['img4id'] != '':
                images.append({"identifier" : row['img4id']}) 

            if len(images) > 0:
                site['images'] = images
                #print(images)

            if row['further_descriptions'] != '':
                site['references'] = Parser.rebuild_refs_sites(row['further_descriptions'], None , None)

            sites.append(site)


        for site in sites:
            if 'references' in site:
                for reference in site['references']:
                    if 'identifier' in reference:
                        if reference['identifier'].startswith('lit'):
                            related_literature_ids.append(reference['identifier'])

        #print(related_literature_ids)
        site_literature = Parser.create_sites_literature(list(set(related_literature_ids)))
        Parser.create_json_file(site_literature, 'site_literature')

        return sites

    @staticmethod
    def create_sites_literature(related_literature_ids):
        literature = []
        for reference in related_literature_ids:
            query = "SELECT * FROM vl_literature WHERE reference='" + reference + "'"
            result = Parser.create_records_db(query, False)
            literature.append(result[0])
        
        return literature



    @staticmethod
    def create_people_old(data):
        people_data = {}
        people_data['name'] = data['shortReference']
        people_data['reference'] = data['reference']
        life_span = Parser.get_life_span(data['shortReference'])
        people_data['life_span'] = life_span
        Parser.post_record(people_data, 'authors')

    # delete?
    @staticmethod
    def get_authors(authors_string, pid, role):
        if authors_string is not None:
            authors_string = authors_string + '\n'
            authors = []
            pattern = re.compile(r"(.*?)\n")
            #x = re.findall(r"(.*?)\n", authors_string)
            # for author in x:
            #    print(author)
            for m in re.finditer(pattern, authors_string):
                contributor = {}
                if pid is not None and pid is not '':
                    contributor['identifier'] = pid
                if role is not None:
                    contributor['role'] = role
                if role is None:
                    contributor['role'] = "author"
                #if authors_string != "\n" and m.group(1) != "" and pid is not None:
                if authors_string != "\n" and m.group(1) != "":
                    contributor['name'] =  m.group(1)
                    print(contributor)
                    authors.append(contributor)


            # print(authors)
            return authors
        #else:
        #    return [{"name": "none"}]

    # delete?
    @staticmethod
    def get_date_span(shortReference):
        life_span = {}
        x = re.search(r"\((\d{4}) - (\d{4})\)", shortReference)
        if x:
            life_span["issued"] = int(x.group(1))
            life_span["year_of_death"] = int(x.group(2))
        else:
            life_span["year_of_birth"] = 0
            life_span["year_of_death"] = 0
        #print(x.group(1) + ' ' + x.group(2))
        return life_span

    # delete?
    @staticmethod
    def get_life_span(shortReference):
        life_span = {}
        x = re.search(r"\((\d{4}) - (\d{4})\)", shortReference)
        if x:
            life_span["year_of_birth"] = int(x.group(1))
            life_span["year_of_death"] = int(x.group(2))
        else:
            life_span["year_of_birth"] = 0
            life_span["year_of_death"] = 0
        #print(x.group(1) + ' ' + x.group(2))
        return life_span

    @staticmethod
    def get_labs_disciplines(labsdisciplines):
        labs_disciplines = []
        pattern = re.compile(r"(.*?)\n")
        for m in re.finditer(pattern, labsdisciplines):
            if m.group() is not '\n':
                labs_disciplines.append({"tag":m.group(1)})
        print(labs_disciplines)
        return labs_disciplines

    @staticmethod
    def rebuild_hyperlinks(pers_career, pid):
        #print(pers_career, pid)
        hit = re.search(r"(<link (.*?)>)(.*?)(<\/link>)", pers_career)
        #pid = None
        #print(pers_career, pid, hit)
        if hit is None:
            return pers_career, pid
        if hit is not None:
            pid = re.search(r"(ref=\"(.*?)\")",
                            hit.group().replace('""', '"')).group(2)
            replacement = hit.group().replace('""', '"').replace('<link ref', '<a href').replace('</link', '</a').replace(pid, Parser.get_url_from_pid(pid))
            pers_career = pers_career[:hit.span()[0]] + \
                    replacement + pers_career[hit.span()[1]:]
            pers_career, pid = Parser.rebuild_hyperlinks(pers_career, pid)
            return pers_career, pid

        #return '', ''
        return Parser.rebuild_hyperlinks(pers_career, pid)

    @staticmethod
    def rebuild_refs_sites(pers_career, references, role):
        hit = re.search(r"(<link (.*?)>)(.*?)(<\/link>)", pers_career)
        if references is None:           
            references = []
        if hit is None and pers_career == '':
            return references
        if hit is None and pers_career != '':
            pattern = re.compile(r"(.*?)\n")
            for m in re.finditer(pattern, pers_career):
                if m.group() is not '\n':
                    reference = dict()
                    reference['name'] = m.group().replace('\n', '')
                    if role is not None:
                        reference['role'] = role
                    references.append(reference)
            return references
        if hit is not None:
            reference = {}
            if role is not None:
                reference['role'] = role
            #print(pers_career, "hit " + str(hit))
            pid_hit = re.search(r"(ref=\"(.*?)\")",
                            hit.group().replace('""', '"'))
            if pid_hit is None:
                return references
            pid = pid_hit.group(2)
            if pid.startswith('per'):
                reference['identifier'] = pid
                reference['$ref'] = "https://vlp.ur.de/api/resolver/author/" + \
                    pid
                pers_career = pers_career[hit.span()[1]:]
                references.append(reference)
                return Parser.rebuild_refs_sites(pers_career, references, role)
            if pid.startswith('lit'):
                reference['identifier'] = pid
                reference['$ref'] = "https://vlp.ur.de/api/resolver/record/" + \
                    pid
                pers_career = pers_career[hit.span()[1]:]
                references.append(reference)
                return Parser.rebuild_refs_sites(pers_career, references, None)
            # TODO Essays!!

            #if pid.startswith('sit') and pid:
            #    reference['$ref'] = "https://vlp.ur.de/api/resolver/site/" + \
            #        pid
            #    pers_career = pers_career[hit.span()[1]:]
            #    references.append(reference)
            #    return Parser.rebuild_refs_sites(pers_career, references, None)
            reference['identifier'] = pid
            replacement = hit.group().replace('""', '"').replace('<link ref', '<a href').replace('</link', '</a').replace(pid, Parser.get_url_from_pid(pid))
            reference['name'] = hit.group(3)
            reference['link'] = replacement
            pers_career = pers_career[hit.span()[1]:]
            references.append(reference)
            return Parser.rebuild_refs_sites(pers_career, references, role)


    @staticmethod
    def get_url_from_pid(pid):
        if pid.startswith('lit'):
            return "/records/" + pid
        if pid.startswith('per'):
            return "/authors/" + pid
        if pid.startswith('sit'):
            return "/sites/" + pid
        return "/records/" + pid

    @staticmethod
    def post_record(json_data, api_root):
        url = Parser.INVENIO_SERVICE_URL + "/api/" + api_root + "/?prettyprint=1"
        data = json.dumps(json_data, ensure_ascii=False).encode('utf-8')
        headers = {'content-type': 'application/json'}
        print("==================================" +
              json_data['identifier'] + "===================================")
        response = requests.post(
            url, data=data, headers=headers, verify=Parser.REQUESTS_SSL_VERIFY)
        if response.ok:
            print("Status code: " + str(response.status_code))
            print(json_data['identifier'] + " - success!")
            print("Message: " + response.text)
        else:
            print("Status code: " + str(response.status_code))
            print("Message: " + response.text)
            print(json.dumps(json_data, sort_keys=True, indent=4))
        print("===================================END====================================")

    @staticmethod
    def create_json_file(records_array, name):

        outfile = open(name + '.json', 'w', encoding='utf-8')
        #print(name)
        data = json.dumps(records_array, ensure_ascii=False, sort_keys=True, indent=4)
        #print(data)
        outfile.write(data)
        outfile.close()
        #json.dump(data, outfile, ensure_ascii=False, indent=4)

    @staticmethod
    def do_reindex_via_cli():
        """
        $ pipenv run invenio index reindex --pid-type recid --yes-i-know
        $ pipenv run invenio index run
        """
        p = os.popen(
            "pipenv run invenio index reindex --pid-type recid --yes-i-know")
        print(p.read())
        p = os.popen("pipenv run invenio index run")
        print(p.read())


if __name__ == "__main__":
    # Parser.parse_file('data/vlp.csv')
    # Parser.parse_people_file('data/VL-People2.csv')
    aut_str = """
Delacroix, Henri
E.Cassirer
L.Jordan
A.Sechehaye
W.Doroszewski
R.A.S.Paget
K.Bühler
H.Pongs
A.Meillet
J.Vendryes
P.Meriggi
V.Broendal
N.Trubetzkoy
Edward Sapir
J.van Ginneken
A.Sommerfelt
A.W.de Groot
O.Jespersen
Ch.Bally
Gustave Guillaume
A.Grégoire
M.Cohen
A.Gelb
K.Goldstein   
"""
    Parser.get_authors(aut_str)

    vlp_links = '1633-1635 studied medicine in Leiden and Basel; M.D. 1637, University of Basel; 1641 moved to Amsterdam and worked there as a physician and was member of the <i>Amsterdam College of Physicians</i>; 1658 professor of medicine, University of Leiden; 1669-1670 vice chancellor of the University; 1671-1674 his main work <i>Praxeos medicae idea nova</i> was published in four volumes." \
"1858 studies of medicine at the University of Breslau under <link ref=""per86"">Rudolf Heidenhain</link> and in Berlin under <link ref=""per64"">Emil Du Bois-Reymond</link>; 1862 medical degree from the University of Berlin with a dissertation on invertebrate muscle physiology; 1864 assistant to <link ref=""per87"">Helmholtz</link> at the <link ref=""sit108"">Physiological Institute of the University of Heidelberg</link>; 1870 Privatdozent; 1871 associate professor and acting head of the Physiological Institute at Heidelberg; later that year return to Berlin; 1872 professor of physiology at the University of Halle in succession to <link ref=""per169"">Friedrich Goltz</link>; founder and director of the physiology institute at Halle (1881)." \
"Studied medicine for several years in Göttingen; 1823 M.D. at the University of Göttingen with a dissertation entitled <i>De cauterio actuali seu de igne ut medicamento</i>; 1824 worked on practical medicine at the University of Berlin and 1825 on zoology and comparative anatomy at the Sorbonne, Paris; 1825 habilitation, University of Göttingen; 1835 associate professor, same institution; 1836 professor of zoology and comparative anatomy at the faculty of medicine, University of Göttingen; since 1840 director of the zoological collection; founded the scientific study of hormones; 1861 retirement.<link ref="lit15702" page="p0007tablevii">Rabbit holder according to Czermak</link>'
    Parser.rebuild_hyperlinks(vlp_links)
