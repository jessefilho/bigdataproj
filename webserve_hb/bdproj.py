from datetime import date

import happybase
import collections
from collections import OrderedDict
import json


connection = happybase.Connection('localhost')
print(connection.tables())


def test(id):
    return json.dumps("Hello "+id)


def question1(id_student,program):
    prfix = b'%s' % (id_student) #b'2017000304' #
    program = b'%s' % (program) #L1
    table = connection.table(b'A_21805893:Q1')

    q1 = []
    first = []
    second = []
    for key, data in table.scan(row_prefix=prfix):

        if(data.get(b'S:PROGRAM') == program):

            if(int(data.get(b'C:CODE')[1:-4])%2 == 0):
                #Second semester
                first.append(dict(Code=data.get(b'C:CODE'),Name=data.get(b'C:NAME'),Grade=data.get(b'C:GRADE')))
            if (int(data.get(b'C:CODE')[1:-4]) %2 != 0):
                # Second semester
                second.append(dict(Code=data.get(b'C:CODE'), Name=data.get(b'C:NAME'), Grade=data.get(b'C:GRADE')))

            q1.append(OrderedDict(Name=data.get(b'S:NAME'),
                                  Email=data.get(b'S:EMAIL'),
                                  Program=data.get(b'S:PROGRAM'),
                                  First=first,
                                  Second=second))

        return json.dumps(q1, sort_keys=False, indent=4)
        #return json.load(q1, object_pairs_hook=OrderedDict)

    #{"Name": "Jean DUPOND", "Email": "jean.dupond@univ-blois.fr", "Program": "M1",
    #"First": [{"Code": "S07A001", "Name": "Big Data", "Grade": "17.5"}, {...}, ...],
    #"Second": [{"Code": "S08A001", "Name": "Database", "Grade": "6.25"}, {...}, ...]}

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

def question4(id_ue,id_year):
    prfix = b'%s' %(id_ue)
    table = connection.table(b'A_21805893:Q4')
    rate = []
    q4 = []
    q4_result = []
    year = id_year

    for key, data in table.scan(row_prefix=prfix+"/"+year):
        q4_result.append(dict(Name = data.get(b'C:NAME'), Rate = float(data.get(b'#:RATE'))))
        rate.append(float(data.get(b'#:RATE')))

    q4.append("("+str(max(rate))+" pour "+str(max(rate)*100)+"% )")
    q4.append(q4_result);
    return json.dumps(q4,sort_keys=True, indent=4)

def question5(program,year):
    table = connection.table(b'A_21805893:Q5')
    q5 = []
    for key, data in table.scan(filter="SingleColumnValueFilter ('result','program',=,'regexstring:^%s')" % (program)):
        if data.get(b'result:year') == year:
            q5.append(key)
            q5.append(dict(Name = data.get(b'result:courseName'), Grade = data.get(b'result:average')))
    return json.dumps(q5, sort_keys=True, indent=4)

def question6(name_prof):
    prfix = b'%s' %(name_prof)
    table = connection.table(b'A_21805893:Q6')

    q6 = []
    q6_result = []

    for key, data in table.scan(row_prefix=prfix):
        q6_result.append(key.split("/")[1]+"/"+ str(9999 - int(key.split("/")[2])));
        q6_result.append(dict(Name=data.get(b'C:NAME'), Rate=float(data.get(b'#:RATE'))))



    q6.append(q6_result);
    return json.dumps(q6,sort_keys=True, indent=4)

def question7(program,year):
    table = connection.table(b'A_21805893:Q7')
    q5 = []
    for key, data in table.scan(filter="SingleColumnValueFilter ('result','promotion',=,'regexstring:^%s')" % (program)):

        if data.get(b'result:year') == year:
            q5.append(key+':'+data.get(b'result:average'))



    return json.dumps(q5, sort_keys=True, indent=4)


#print question7('M1','2007');