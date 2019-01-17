import happybase
import collections
import json

connection = happybase.Connection('localhost')
print(connection.tables())


def test(id):
    return json.dumps("Hello "+id)


def question1():
    print "question1"

def question2(semester):
    prfix = b'%s' % (semester)
    table = connection.table(b'A_21805893:Q2')
    rate = []
    q2 = []
    q2_result = []
    for key, data in table.scan(row_prefix=prfix):
        q2_result.append(dict(Year=key.split('/')[1], Rate=float(data.get(b'result:rate'))))
        rate.append(float(data.get(b'result:rate')))


    q2.append("(" + str(max(rate)) + " pour " + str(max(rate) * 100) + "% )")
    q2.append(q2_result);
    return json.dumps(q2, sort_keys=False, indent=4)


#(0.88 pour 88%) :
#[{"Year":"2001","Rate":"0.88"},{"Year":"2002","Rate":"0.83"},...]

def question3(id_ue):
    prfix = b'%s' %(id_ue)
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


#print question2(str(S07));