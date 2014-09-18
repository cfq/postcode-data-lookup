import psycopg2
import json, requests
import random

from itertools import izip_longest

conn = psycopg2.connect("dbname='courtfinder_production' user='courtfinder' host='localhost'")

# postcodes_tried = {}
# postcode_not_found = []
# courts = {}
# local_authorities = {}

mapit_full_url = "http://mapit.mysociety.org/postcode/%s"
mapit_partial_url = "http://mapit.mysociety.org/postcode/partial/%s"


def get_all_postcodes_db():
    cur = conn.cursor()
    cur.execute("""
        SELECT c.name, pc.postcode
          FROM postcode_courts as pc,
               courts as c
         WHERE pc.court_id = c.id
    """)

    rows = cur.fetchall()
    return [r for r in rows]


def get_all_postcodes():
    with open('out/all_pcodes.json', 'r') as postcode_file:
        all_ps = json.load(postcode_file)
        postcode_file.close()

    return all_ps


def write_to_json( filename, obj ):
    with open('out/%s.json' % filename, 'w') as outfile:
        json.dump(obj, outfile, indent=4, separators=(',', ': '))
        print "== out/%s.json written" % filename
        outfile.close()


def format_postcode( postcode ):
    return postcode.lower().replace(' ', '')


def mapit( postcode, ptype="full" ):
    if not (ptype == 'full' or ptype == 'partial'):
        print "MapIt type should be 'partial' or 'full', sent: %" % ptype
        return False

    p = format_postcode(postcode)

    url = (mapit_full_url, mapit_partial_url)[ptype == 'partial']
    r = requests.get(url % p)
    if r.status_code == 200:
        data = json.loads(r.text)
        
        if ptype == 'partial':
            return False

        if ptype == 'full':
            if type(data['shortcuts']['council']) == type({}):
                council_id = str(data['shortcuts']['council']['county'])
            else:
                council_id = str(data['shortcuts']['council'])

            return data['areas'][council_id]['name']
    else:
        data = json.loads(r.text)
        print "%s - MapIt %s postcode Error: %s" % (postcode, ptype, data['error'])
        return False




def postcodes_io_bulk( postcodes ):
    ps = [format_postcode(p) for p in postcodes]

    payload = { "postcodes": ps }
    r = requests.post('http://api.postcodes.io/postcodes', data=payload )

    if r.status_code == 200:
        response = json.loads(r.text)
        if response['status'] == 200:
            good_postcodes = {}
            bad_postcodes = []

            for result in response['result']:
                postcode = result['query']
                print postcode
                if result['result'] == None:
                    bad_postcodes.append(postcode)
                else: 
                    if result['result']['admin_county'] is not None:
                        council_name = result['result']['admin_county']
                    elif result['result'] is not None:
                        council_name = result['result']['admin_district']
                    else:
                        print "--> Neither: %s" % postcode

                    good_postcodes[postcode] = council_name

            return {
                "good_postcodes": good_postcodes,
                "bad_postcodes": bad_postcodes
            }
        else:
            print "Postcodes.io Error: %s"  % r.status
            print r.text
    else:
        print "Postcodes.io Error: %s" % r.text




def match_postcodes():
    all_postcodes = get_all_postcodes()

    for court, postcode in all_postcodes:
        la = postcode_to_local_authority(postcode)

        print "%s -- %s -- %s" % (court, postcode, la)

        if la in local_authorities:
            local_authorities[la].append(postcode)
        else:
            local_authorities[la] = [postcode]

        if court in courts:
            courts[court].append(la)
        else:
            courts[court] = [la]


    write_to_json( 'courts', courts )
    write_to_json( 'local_authorities', local_authorities )


if __name__ == '__main__':
    pcodes = [c[1] for c in get_all_postcodes()]
    plist = [[p for p in pbulk if p is not None]  for pbulk in izip_longest(*[iter(pcodes)]*100)]

    goodlist = {}
    badlist = []

    total_count = len(plist)
    for idx, pbulk in enumerate(plist):
        print " ---- %s of %s" % (idx+1, total_count)
        results = postcodes_io_bulk(pbulk)
        goodlist.update(results['good_postcodes'])
        badlist += results['bad_postcodes']

    write_to_json('postcode_io_good', goodlist)
    write_to_json('postcode_io_bad', badlist)
