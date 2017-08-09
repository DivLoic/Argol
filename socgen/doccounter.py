# coding: utf-8
import os
import re
import sys
import json
import yaml
import math
import urllib3
import pandas as pd

from datetime import datetime

#import pprint
#pp = pprint.PrettyPrinter(indent=1)

reload(sys)
sys.setdefaultencoding('utf8')

urllib3.disable_warnings()
from openpyxl import load_workbook
from optparse import OptionParser

CONF = dict()
HERE = os.environ['APP_EXE'] + '/doccounter/'
HTTP = urllib3.PoolManager()

KEYS = ["start", "end"]
ENV = ["ES_HTTPS_ENDPOINT", "APP_USER", "TYPEDOCS", "EMISSION", "APP_ES_PASSWD", "APP_LOG_ALIM"]
QUERY = "{\"query\":{\"term\":{\"index\":\"(...)\"}}, \"sort\":[{\"ordre\":{\"order\":\"desc\"}}]}"
GRID_INI_DF = {"source": [], "fichier": [], "Date reception": []}
HEADER_SUIVI = { "timestamp": "Date dâintegration" }
HEADER_SUIVI_LIST = [
        "Nom du fichier","Source","Type client",
        "Date reception","Chargement","Date dâintegration",
        "Date de derniere integration"
]

# fonction utils
def idxToSource(idx):
        if 'bel' in idx:
                return 'bdr'
        else:
                return idx[5:]

def sourceToIndex(src):
        mapping = {"dun": "DUN","rct": "RCT", "bdr": ("BDRLE", "BDREL"), "grc": "GRC", "autres": "FORCE"}
        if src == "bdr":
                le, el = mapping["bdr"]
                return os.environ[le] +","+ os.environ[el].lower()
        else:
                source = mapping[src]
                return os.environ[source].lower()

def noSuchFile(path):
        if not os.path.isfile(path) :
                print "Le fichier : %s n'a pas Ã©tÃ© retrouvÃ©"%(path)
                sys.exit(1)

def es_mock():
        return 0, 0

def pickupDate(filename):
        topick = "[0-9]{4}-[0-9]{2}-[0-9]{2}"
        res = re.search(topick,filename)
        word = res.group(0)
        return datetime.strptime(word, "%Y-%m-%d").date()

def dig(array):
    """ """
    values = []
    for key in array.keys():
        if type(array[key]) is list:
            for a in array[key]:
                values += dig({key: a})
            pass
        elif type(array[key]) is dict:
            values += dig(array[key])
        else:
            values.append({key: array[key]})
    return values

def extract(chunk):
        if(len(chunk) >= 1):
                chunk = chunk[0]
                datum = dig(chunk)
                datum = map(lambda y: (y.keys()[0], y[y.keys()[0]]), datum)
                datum = filter(lambda y: y[0] in KEYS, datum)
                datum = datum = map(lambda y: int(y[1]), datum)
        else:
                datum = (-1, -1)
        return datum

def getAllFilesIn(path):
        all_files = [f for f in os.listdir(path) if os.path.isfile(path +'/'+ f)]
        return all_files

def selectFile(sourcename):
        log_path = os.environ['APP_LOG_ALIM']
        log_files = getAllFilesIn(log_path)
        log_files = filter(lambda y : y.find('kyc_alimentation') != -1, log_files)
        log_files = filter(lambda y : y.find(sourcename) != -1, log_files)
        log_files.sort()
        return log_files

def targetFile(sourcename, inline=False):
        if inline:
                pass
        else:
                log_path = os.environ['APP_LOG_ALIM']
                log_files = getAllFilesIn(log_path)
                log_files = filter(lambda y : y.find('kyc_alimentation') != -1, log_files)
                log_files = filter(lambda y : y.find(sourcename) != -1, log_files)
                log_files.sort(reverse=True)

                if len(log_files) > 0:
                        path = log_path + '/' + log_files[0]
                else:
                        sys.exit('Aucun ficher de log \'a Ã©tÃ© rÃ©cupÃ©rÃ© !')

        return path

def loadRecords(path, sourcename):
        records = list()
        with open(path, "r") as logs:
                filename = ""
                src = {}
                for log in logs:
                        if sourcename != 'bdr':
                                if CONF["file"] in log:
                                        # dernier mot Ã  la ligne source
                                        filename = log.split(' ')[-1].strip(' \n ')#\n temporary
                                        if(filename[-4:] == ".csv"): filename = filename[:-4]
                        else:
                                if 'Dezippage' in log:
                                        filename += log.split(' ')[-2].strip(' \n ') + ' \n'

                        if CONF['add'] in log:
                                splited = log.split(' ')
                                src["Date d'intÃ©gration"] = splited[0]+' '+splited[1]
                                docs = re.findall("=(\d+)", log)
                                if len(docs) == 2:
                                        src['ajoutÃ©'] = docs[0]
                                        src['supprimÃ©'] = docs[1]
                                        records.append(src.copy())
                                        src = {}

                        if CONF["end"] in log:
                                # deux resultats aprÃ¨s symbole "="
                                #"([-+]?[0-9]*\.?[0-9]+[eE][-+]?[0-9]+?)"
                                src['source'] = idxToSource(options.index)
                                docs = re.findall("=(\d+)", log)
                                if len(docs) == 2:
                                        src['fichier'] = filename
                                        src['avant'] = float(docs[0])
                                        src['aprÃ¨s'] = float(docs[1])

        return records

def loadTracker(eshost, esuser, typeindex, head):
        res = HTTP.request("GET", eshost+"/"+typeindex+"/_search?size=30000", body="", headers=head)
        hits = []
        try:
                doc = json.loads(res.data)
                hits = doc["hits"]["hits"]
        except:
                print "Erreur de parsing json de suivi"
                print doc

        source = map(lambda y: y["_source"], hits)

        df_ini = pd.DataFrame(source)#"source","type","load","timestamp","filename"
        df_ini["timestamp"] = df_ini["timestamp"].apply(lambda y: datetime.strptime(y[:19], "%Y-%m-%dT%X").strftime("%Y-%m-%d %X"))
        df_ini.sort(["timestamp"])
        df_left = df_ini.groupby(["type"]).tail(2).sort(["type", "timestamp"])
        df_left = df_left.groupby(["type"]).tail(1)

        df_right = pd.DataFrame(df_left.groupby(["type"])["timestamp"].min())
        df_right.reset_index(level=0, inplace=True)
        df_right = df_right.rename(columns={'timestamp': 'last_timestamp'})

        df_final = pd.merge(df_left, df_right, on="type", how='outer')

        # FORMATAGE:
        df_final["load"] = df_final["load"].map({True: "OK", False: "NOK"})
        df_final["reception_date"] = df_final["filename"].apply(receptionDate)


        df_final = df_final.rename(columns=HEADER_SUIVI)

        return df_final

def columnBuild(df):
        try:
                df["avant"] = df["avant"].apply(int)
                df["aprÃ¨s"] = df["aprÃ¨s"].apply(int)
                df["ajoutÃ©"] = df["ajoutÃ©"].apply(int)
                df["supprimÃ©"] = df["supprimÃ©"].apply(int)
        except:
                print "Fichier de log vide."
                return pd.DataFrame(GRID_INI_DF)

        df["delta"] = (df["aprÃ¨s"] -  df["avant"]) - (df["ajoutÃ©"] -  df["supprimÃ©"])

        df["statut"] = df.apply(lambda y: "OK" if y["delta"] == 0 else "NOK", axis=1)

        df["Date reception"] = df["fichier"].apply(receptionDate)

        return df

def getDdate(ddate):
        d = "010101"
        thedate = datetime.strptime(d, "%y%m%d").date()
        try:
                thedate = datetime.strptime(ddate, "D%d%m%y").date()
        except:
                try:
                        thedate = datetime.strptime(ddate, "D%d%m%Y").date()
                except:
                        thedate = datetime.strptime(d, "%y%m%d").date()

        return thedate.strftime("%Y-%m-%d")


def receptionDate(title):
        dates = re.findall(r"D(\d+)",title)
        date = "D"+dates[0] if len(dates) == 1 else "D010101"
        return getDdate(date)

def multichoise(options,question="Choisissez une proposition"):
        assert type(options) is list
        print question
        for (o, option) in enumerate(options):
                print "%i) %s"%(o, option)

        ans = raw_input(" : ")
        try:
                number = int(ans)
                assert number <= len(options)
        except:
                return multichoise(options,question="Choisissez une proposition")

        print options[number]
        return options[number]

if __name__ == "__main__":

        # lookup env
        for e in ENV:
                if e not in os.environ.keys():
                        print "La variable d'environement %s est manquante"%e
                        sys.exit(1)

        eshost = os.environ["ES_HTTPS_ENDPOINT"]
        esuser = os.environ["APP_USER"]
        typedoc = os.environ["TYPEDOCS"]

        password_file=open(os.environ["APP_ES_PASSWD"]+"/es_passwd.txt")
        password=re.findall("passwd=(.*)",password_file.readline())[0]
        password_file.close()

        # lookup conf
        with open(HERE + "/doccounter.yaml", "r") as y:
                CONF = yaml.load(y)

        # lookup args
        optparser = OptionParser()
        optparser.add_option("-m", "--memo", dest="memo", default="", help="fichier contenant le chemin vers les logs")
        optparser.add_option("-n", "--namespace", dest="namespace", default="", help="type de source (bdr, rct, grc, autres)")
        optparser.add_option("-i", "--index", dest="index", default="", help="index elasticsearch de l'integration")
        optparser.add_option("-l", "--inline", dest="inline", default=False, help="utilisation ou non du mode inlinen")

        options, args = optparser.parse_args()

        # inline or not ?
        if(options.namespace == "inline"):
                selcted_source = multichoise(["rct", "bdr", "grc", "dun","autres"], question="Choisissez une source : ")
                selcted_file = multichoise(selectFile(selcted_source), question="Choisissez un fichier : ")
                final_file_path = os.environ['APP_LOG_ALIM'] + "/" + selcted_file
                selcted_date = pickupDate(final_file_path)

                options.namespace = selcted_source
                options.index = sourceToIndex(selcted_source)
       else:
                selcted_date = datetime.now()
                final_file_path = targetFile(sourcename=options.namespace)


        QUERY = QUERY.replace("(...)", options.index)

        # lookup existing .xlsx
        xls_file_name = os.environ['EMISSION'] + '/comptage_%s.xlsx'%(selcted_date.strftime("%Y-%m-%d"))
        if(os.path.isfile(xls_file_name)):
                book = load_workbook(xls_file_name)
                writer = pd.ExcelWriter(xls_file_name, engine='openpyxl')
                writer.book = book
                writer.sheets = dict((ws.title, ws) for ws in book.worksheets)# if ws.title != "Suivi")
        else:
                writer = pd.ExcelWriter(xls_file_name, engine='openpyxl')

        noSuchFile(final_file_path)
        noSuchFile(HERE + "/doccounter.yaml")

        # lookup eslasticsearch alim count
        credentials = "superuser:"+password
        head = urllib3.util.make_headers(basic_auth=credentials)
        res = HTTP.request("GET", eshost+"/"+esuser+"/"+typedoc+"/_search", body=QUERY, headers=head)

        try:
                doc = json.loads(res.data)
        except Exception as e:
                print "Le document elasticsearch de sortie est erone ou elasticsearch est inaccessible."
                sys.exit(1)

        if "hits" in doc.keys():
                # standard api seach elasticsearch _shards > hits > hits
                INITIALCOUNT, FINALCOUNT = extract(doc["hits"]["hits"])
        else:
                INITIALCOUNT, FINALCOUNT = es_mock()

        # process
        print "utilisation du fichier : ",final_file_path
        records = loadRecords(final_file_path, sourcename=options.namespace)
        
         df = pd.DataFrame(records)
        df = columnBuild(df)

        if len(df) > 0:
                nbErr = len(df[df['statut'] == 'NOK'])
                err = "KO" if nbErr == 0 else "OK"

        diff = (df.sum(axis=0)["aprÃ¨s"] - df.sum(axis=0)["avant"]) - (FINALCOUNT - INITIALCOUNT)

        df_suivi = loadTracker(eshost, esuser, os.environ["KYC_SUIVI_INDEX"].lower(), head)

        df_suivi.to_excel(writer, sheet_name="Suivi", encoding="utf-8", index=False, header=True, columns=HEADER_SUIVI_LIST)

        df.to_excel(writer, sheet_name=idxToSource(options.index), encoding="utf-8", index=False, header=True,
                columns=["source", "fichier", "Date reception","Date d'intÃ©gration", "avant", "aprÃ¨s", "ajoutÃ©","supprimÃ©", "delta", "statut"])

        writer.save()
        print "Retrouvez le fichier rÃ©capitulatif: %s"%xls_file_name


