import logging
logging.basicConfig(level=logging.DEBUG)

from spyne import Application, rpc, ServiceBase, \
    Integer, Unicode, Float

from spyne import Iterable

from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument

from spyne.server.wsgi import WsgiApplication
import time
import json
import urllib2
import datetime
from time import mktime
import dateutil.parser as dparser
import requests
import operator
import re
from datetime import date

addr_start_list =[ "S", "E", "SE","N","W","NE"]
addr_end_list = [ "ST" ,"AVE" ,"RD" ,"AV" ,"WY" ,"WAY" ,"BL" , "STN"]



class CrimeReportService(ServiceBase):

    @rpc(Float, Float, Float, _returns=Unicode)
    def checkcrime(self, lat, lon, radius):

        ret_response = {}
        base_url = "https://api.spotcrime.com/crimes.json"
        get_url = "%s?lat=%s&lon=%s&radius=%s&key=." % (base_url, lat, lon, radius)
        print get_url
        #json_data = json.loads(urllib2.urlopen(get_url))

        isDoubleAddr = False
        get_response = requests.get(get_url)
        json_data = json.loads(get_response.content)
        if get_response.status_code == 200:
            print  len(json_data['crimes'])
            print  json_data.get("crimes")

        crime_type_count_dict = {}
        json_output = {
            "total_crime": 0,
            "the_most_dangerous_streets": [],
            "crime_type_count": {},
            "event_time_count": {
                "12:01am-3am": 0,
                "3:01am-6am": 0,
                "6:01am-9am": 0,
                "9:01am-12noon": 0,
                "12:01pm-3pm": 0,
                "3:01pm-6pm": 0,
                "6:01pm-9pm": 0,
                "9:01pm-12midnight": 0
            }
        }
        if get_response.status_code == 200:
            for obj in json_data['crimes']:
                # cehck address
                if(obj['address'].find(" & ") != -1 or obj['address'].find(" AND ") != -1 ):
                    isDoubleAddr = True
                    print " address : %s "% obj['address']
                    if(obj['address'].find(" AND ") != -1 ):
                        addr_str = obj['address'].split(' AND ')
                    else:
                        addr_str = obj['address'].split(' & ')

                    print re.search(r'\b\d{1,3}(?:\s[a-zA-Z\u00C0-\u017F]+)+', addr_str[0])
                    if(crime_type_count_dict.has_key(addr_str[0])):
                        addr_cnt = crime_type_count_dict.get(addr_str[0])
                        addr_cnt = addr_cnt +1
                        crime_type_count_dict.update({addr_str[0]: addr_cnt})
                    else:
                        crime_type_count_dict.update({addr_str[0]: 1})
                    if (crime_type_count_dict.has_key(addr_str[1])):
                        addr_cnt = crime_type_count_dict.get(addr_str[1])
                        addr_cnt = addr_cnt + 1
                        crime_type_count_dict.update({addr_str[1]: addr_cnt})
                    else:
                        crime_type_count_dict.update({addr_str[1]: 1})
                else:
                    if (crime_type_count_dict.has_key(obj['address'])):
                        addr_cnt = crime_type_count_dict.get(obj['address'])
                        addr_cnt = addr_cnt + 1
                        crime_type_count_dict.update({obj['address']: addr_cnt})
                    else:
                        crime_type_count_dict.update({obj['address'] : 1})

                # check type
                if (json_output["crime_type_count"].has_key(obj['type'])):
                    val = json_output["crime_type_count"].get(obj['type'])
                    if isDoubleAddr:
                        val = val + 2
                    else:
                        val = val + 1;
                    json_output["crime_type_count"].update({obj['type']: val})
                else:
                    if isDoubleAddr:
                        json_output["crime_type_count"].update({obj['type']: 2})
                    else:
                        json_output["crime_type_count"].update({obj['type']: 1})


                # cehck time
                obj_date = dparser.parse(obj['date'])
                obj_date.strftime('%m/%d/%y %H:%M:%S')
                print " object date time :%s"%obj_date.time()
                time_slot = obj_date.time()
                print time_slot.strftime('%H:%M:%S')

                #self.settimeslot(obj['date'])
                if (time_slot >= datetime.time(0, 1) and time_slot <= datetime.time(3, 0)):
                    val = json_output["event_time_count"].get("12:01am-3am")
                    if isDoubleAddr:
                        val = val + 2
                    else:
                        val = val + 1
                    json_output["event_time_count"]["12:01am-3am"] = val
                if (time_slot >= datetime.time(3, 1) and time_slot <= datetime.time(6, 0)):
                    val = json_output["event_time_count"].get("3:01am-6am")
                    if isDoubleAddr:
                        val = val + 2
                    else:
                        val = val + 1
                    json_output["event_time_count"]["3:01am-6am"] = val
                if (time_slot >= datetime.time(6, 1) and time_slot <= datetime.time(9, 0)):
                    val = json_output["event_time_count"].get("6:01am-9am")
                    if isDoubleAddr:
                        val = val + 2
                    else:
                        val = val + 1
                    json_output["event_time_count"]["6:01am-9am"] = val
                if (time_slot >= datetime.time(9, 1) and time_slot <= datetime.time(12, 0)):
                    val = json_output["event_time_count"].get("9:01am-12noon")
                    if isDoubleAddr:
                        val = val + 2
                    else:
                        val = val + 1
                    json_output["event_time_count"]["9:01am-12noon"] = val
                if (time_slot >= datetime.time(12, 1) and time_slot <= datetime.time(15, 0)):
                    val = json_output["event_time_count"].get("12:01pm-3pm")
                    if isDoubleAddr:
                        val = val + 2
                    else:
                        val = val + 1
                    json_output["event_time_count"]["12:01pm-3pm"] = val
                if (time_slot >= datetime.time(15, 1) and time_slot <= datetime.time(18, 0)):
                    val = json_output["event_time_count"].get("3:01pm-6pm")
                    if isDoubleAddr:
                        val = val + 2
                    else:
                        val = val + 1
                    json_output["event_time_count"]["3:01pm-6pm"] = val
                if (time_slot >= datetime.time(18, 1) and time_slot <= datetime.time(21, 0)):
                    val = json_output["event_time_count"].get("6:01pm-9pm")
                    if isDoubleAddr:
                        val = val + 2
                    else:
                        val = val + 1
                    json_output["event_time_count"]["6:01pm-9pm"] = val
                if( time_slot >= datetime.time(21,1) and ( time_slot <= datetime.time(23,59)) or time_slot == datetime.time(0,0) ):
                    val = json_output["event_time_count"].get("9:01pm-12midnight")
                    if isDoubleAddr:
                        val = val + 2
                    else:
                        val = val + 1
                    json_output["event_time_count"]["9:01pm-12midnight"] = val

                isDoubleAddr = False; #end of for loop

        crime_type_count_dict_sorted =  sorted(crime_type_count_dict,  key=crime_type_count_dict.__getitem__, reverse=True)
        cnt = 0;
        for i in crime_type_count_dict_sorted:
            addr_slot = i.split()

            print " street : %s" % i[3:len(i)]
            plain_addr = []
            for c in addr_slot:
                if not c.isdigit():
                    plain_addr.append(c+" ")

            result = ''.join(plain_addr)
            result = result.replace("OF","")
            result = result.replace("BLOCK", "")
            print " street : %s" % result.lstrip()
            json_output["the_most_dangerous_streets"].append(result.lstrip())
            cnt = cnt + 1
            if(cnt == 3):
                break

        total_crime = json_output["event_time_count"]["12:01am-3am"] + json_output["event_time_count"]["3:01am-6am"] + json_output["event_time_count"]["6:01am-9am"] + json_output["event_time_count"]["9:01am-12noon"] +\
                      json_output["event_time_count"]['12:01pm-3pm'] + json_output["event_time_count"]["3:01pm-6pm"] + json_output["event_time_count"]['6:01pm-9pm'] + json_output["event_time_count"]["9:01pm-12midnight"]
        json_output['total_crime'] = total_crime

        if(total_crime == 0):
            json_output["event_time_count"] = { }

        print crime_type_count_dict
        #res = Response(json.dumps(json_output), status=200)
        #return res
        return json_output

    def testme(self):
        print " function tested"


application = Application([CrimeReportService],
    tns='spyne.examples.crimereport',
    in_protocol=HttpRpc(validator='soft'),
    out_protocol=JsonDocument()
)

if __name__ == '__main__':
    # You can use any Wsgi server. Here, we chose
    # Python's built-in wsgi server but you're not
    # supposed to use it in production.
    from wsgiref.simple_server import make_server

    wsgi_app = WsgiApplication(application)
    server = make_server('127.0.0.1', 8000, wsgi_app)
    server.serve_forever()