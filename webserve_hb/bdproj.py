import happybase
import collections
import json

connection = happybase.Connection('localhost')
print(connection.tables())


def test(id):
    return json.dumps("Hello "+id)


def question1():
    print "question1"


def question3(id):
    prfix = b'%s' %(id)
    table = connection.table(b'A_21805893:Q3')
    rate = []
    q3 = []
    q3_result = []
    for key, data in table.scan(row_prefix=prfix):
        q3_result.append(dict(Name = data.get(b'#:NAME'), Rate = float(data.get(b'#:RATE'))))
        rate.append(float(data.get(b'#:RATE')))

    q3.append("("+str(max(rate))+" pour "+str(max(rate)*100)+"% )")
    q3.append(q3_result);
    return json.dumps(q3,sort_keys=True, indent=4)

    #MODEL (0.88 pour 88%) :[{"Name": "HPC", "Rate": "0.183"}, {"Name": "Big Data", "Rate": "0.88"}, ...]


#print question3("S01A005");