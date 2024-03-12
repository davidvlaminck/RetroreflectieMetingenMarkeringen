# -------------------------------------------------------------------------------
# Name:        inlezen mobiele retrorefletometingen van Goolge Team Drive
# Purpose:      Dit programma leest de .xls bestanden vanop de drive in en
#               voegt die toe in een centrale POSTGIS-databank
#
# Author:      deblochf
#
# Created:     31/08/2017
# Copyright:   (c) deblochf 2017
# Licence:     AWV-EVT-deblochf
# -------------------------------------------------------------------------------

# H:\Drives van mijn team\Systematische Retroreflectiemetingen\Meetjaar 2017
# H:\Drives van mijn team\Databeheer & ICT\test

import datetime
import os
import psycopg2
import shutil
import smtplib
import time

# ingeven meetjaar
meetjaar = 2022

splittrigger = 'Meetjaar ' + str(meetjaar)

host = 'localhost'
port = '5432'
dbname = 'evt2'
user = 'tvpl'
pasword = 'tvpl'
PostgisConnString = ["dbname=" + dbname + " user=" + user + " host=" + host + " port=" + port + " password=" + pasword,
                     "dbname=" + dbname + " user=" + user + " host=" + host + " port=" + port + " password=" + pasword,
                     "dbname=" + dbname + " user=" + user + " host=" + host + " port=" + port + " password=" + pasword,
                     "dbname=" + dbname + " user=" + user + " host=" + host + " port=" + port + " password=" + pasword]
ConnString = 0

PostgisTabelName = f'ttw_t_import_mobiele_retroreflectometer{meetjaar}'
PostgisLOGTabel = f'ttw_log_import_mobiele_retroreflectometer{meetjaar}'

mxdmap = 'C:\\Users\\deblochf\\Google Drive\\EVT teams\\ToepassingWegen\\Markeringen\\Mobiele Retroreflectometingen\\MXD' + str(
    meetjaar)  # is niet meer in gebruik , was onder ArcMap om direct pdf's aan te maken
# mxdmap='H:\\Mijn Drive\\EVT teams\\ToepassingWegen\\Markeringen\\Mobiele Retroreflectometingen\\MXD\\'+str(meetjaar)
##mxdname='2017_metingen.mxd'
##mxdurl=os.path.join(mxdmap,mxdname)


##definitieve link
# url= 'H:\\Drives van mijn team\\Systematische Retroreflectiemetingen\\Meetjaar 2018'
url = 'H:\\Gedeelde drives\\Systematische Retroreflectiemetingen\\Meetjaar ' + str(meetjaar)

##definitieve link
# Scan_folder = ["H:\\Drives van mijn team\\Systematische Retroreflectiemetingen\\Meetjaar 2019"]
Scan_folder = ["H:\Gedeelde drives\Systematische Retroreflectiemetingen\Meetjaar " + str(meetjaar)]

##testlinken
# Scan_folder = ["C:\\Users\\deblochf\\Google Drive\\EVT teams\\ToepassingWegen\\Markeringen\\Mobiele Retroreflectometingen\\Meetjaar 2018"]
# Scan_folder = ["C:\\Users\\deblochf\\Google Drive\\EVT teams\\ToepassingWegen\\Markeringen\\TESTMAP\\input"]
# Scan_folder = ["H:\\Mijn Drive\\EVT teams\\ToepassingWegen\\Markeringen\\TESTMAP\\input"]

# Scan_folder=["D:\\Gebruikersgegevens\\deblochf\\Downloads\\test"]

print(Scan_folder[0])
top = Scan_folder[0]
startdatum = str(time.strftime('%Y-%m-%d %H:%M:%S'))
bestandtypes = [
    '.xls']  # add filetypes or as required. ('.pdf','jpg') '' for all files (remember .shp has 4 or 5 other associated files)
zoektext = ['']  # add strings as required use format ['search1','search2'] etc.

sql_delete_fouten = """ delete from """ + PostgisTabelName + """ where bestands_url IN ( SELECT bestands_url FROM """ + PostgisLOGTabel + """ where ctr_header_bestandsnaam like '%NIET%'); """

sql_delete_logfouten = """ delete from """ + PostgisLOGTabel + """ where ctr_header_bestandsnaam like '%NIET%'; """

sql_list_ingelezenbestanden = """ SELECT bestands_url FROM """ + PostgisLOGTabel + """ order by bestands_url;"""

##SQLMAPfile='C:\\Users\\deblochf\\Google Drive\\EVT teams\\ToepassingWegen\\Markeringen\\Mobiele Retroreflectometingen\\SQL_POSTGIS\\update tabel met geometrie.sql'
# maak tabel met meetjaar aan
sql_create_table = """create table if not exists ttw_t_mobiele_retroreflectometer_""" + str(meetjaar) + """ as
SELECT	oid,bestands_url, toestel, meting_serie_naam,left(meting_serie_naam,8) as ident8,
    markering, tijdstip_meting ,locatieomschrijving, operator_naam, kmp, rl,rlmax,rlmin,rlprocpass,rlstddev,vochtigheid,snelheid,marker,
    latitude_start,longitude_start,latitude_eind,longitude_eind,
	ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370)as geomL72,
	ST_X(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))::INT as X_coord,
	ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))::INT as Y_coord,
	 concat(latitude_start,',',longitude_start) as start_coord,
     concat(latitude_eind,',',longitude_eind) as eind_coord,
     inleesdatum
FROM """ + str(PostgisTabelName) + """
where date_part('year',tijdstip_meting) = """ + str(meetjaar) + """;
"""
# vul tabel aan met
sql_update_insert = """insert into ttw_t_mobiele_retroreflectometer_""" + str(meetjaar) + """
SELECT	a.oid, a.bestands_url, toestel, meting_serie_naam,left(meting_serie_naam,8) as ident8,
    markering, tijdstip_meting ,locatieomschrijving, operator_naam, kmp, rl,rlmax,rlmin,rlprocpass,rlstddev,vochtigheid,snelheid,marker,
    latitude_start,longitude_start,latitude_eind,longitude_eind,
	ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370)as geomL72,
	ST_X(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))::INT as X_coord,
	ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))::INT as Y_coord,
	 concat(latitude_start,',',longitude_start) as start_coord,
     concat(latitude_eind,',',longitude_eind) as eind_coord,
     a.inleesdatum
FROM """ + str(PostgisTabelName) + """ a
JOIN """ + str(PostgisLOGTabel) + """ b on b.bestands_url=a.bestands_url
where ctr_header_bestandsnaam ='OK' and date_part('year',tijdstip_meting) =""" + str(
    meetjaar) + """ and (a.inleesdatum >(SELECT max( inleesdatum) FROM ttw_t_mobiele_retroreflectometer_""" + str(
    meetjaar) + """));
"""

sql_make_line = """create table if not exists ttw.ttw_t_retroreflectometingen_""" + str(meetjaar) + """_line as
select oid,bestands_url,toestel, meting_serie_naam, marker,markering, tijdstip_meting ,locatieomschrijving, operator_naam, ident8,kmp,	rl,rlmax,rlmin,rlprocpass,rlstddev,vochtigheid,
CASE when left(markering,2) ='M1' and right(ident8,1) ='1' then 2000
		when left(markering,2) ='M2' and right(ident8,1) ='1' then 1000
        when left(markering,2) ='M1' and right(ident8,1) ='2' then -2000
        when left(markering,2) ='M2' and right(ident8,1) ='2' then -1000
        end as kaart_offset,
	snelheid,inleesdatum,
    ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370)) as geom_line,
    ST_length(ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))) as lengte
    from ttw_t_mobiele_retroreflectometer_""" + str(meetjaar) + """
where  1=2;

--alter table ttw_t_retroreflectometingen_""" + str(
    meetjaar) + """_line ADD constraint ttw_t_retroreflectometingen_""" + str(meetjaar) + """_line_pkey primary key(oid);

--alter table ttw_t_retroreflectometingen_""" + str(
    meetjaar) + """_line ADD constraint ttw_t_retroreflectometingen_""" + str(meetjaar) + """_line_geom_check CHECK (st_srid(geom) = 31370);

insert into ttw.ttw_t_retroreflectometingen_""" + str(meetjaar) + """_line
select oid, bestands_url,toestel, meting_serie_naam, marker,markering, tijdstip_meting ,locatieomschrijving, operator_naam, ident8,kmp,	rl,rlmax,rlmin,rlprocpass,rlstddev,vochtigheid,
CASE 	when left(markering,2) ='M1' and right(ident8,1) ='1' then 2000
		when left(markering,2) ='M2' and right(ident8,1) ='1' then 1000
        when left(markering,2) ='M1' and right(ident8,1) ='2' then -2000
        when left(markering,2) ='M2' and right(ident8,1) ='2' then -1000
        end as kaart_offset,
	snelheid,inleesdatum,--(select max(inleesdatum)from ttw_t_retroreflectometingen_""" + str(meetjaar) + """_line) as maxinleesdatum,
    ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370)) as geom_line,
    ST_length(ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370))) as lengte
    from ttw_t_mobiele_retroreflectometer_""" + str(meetjaar) + """
where  (ST_length(ST_Makeline(ST_Transform(ST_SetSRID(ST_MakePoint(longitude_eind,latitude_eind),4326),31370) ,ST_Transform(ST_SetSRID(ST_MakePoint(longitude_start,latitude_start),4326),31370)))between 0 and 100)
		and ( inleesdatum>(select max(inleesdatum)from ttw_t_retroreflectometingen_""" + str(
    meetjaar) + """_line) or ((select count(inleesdatum) from ttw_t_retroreflectometingen_""" + str(meetjaar) + """_line) < 1 ));

"""

sql_naamfout = """SELECT bestands_url
	FROM """ + str(PostgisLOGTabel) + """
    WHERE ctr_header_bestandsnaam like '%NIET%';
"""

sql_count_naamfout = """SELECT count(oid)
	FROM """ + str(PostgisLOGTabel) + """
    WHERE ctr_header_bestandsnaam like '%NIET%';
"""


def copybestand(inputfile, outputfile):
    shutil.copyfile(inputfile, outputfile)


def verstuurmail(mail_hostname, sender, receiverlist, inhoud):
    smtpObj = smtplib.SMTP(mail_hostname)
    smtpObj.sendmail(sender, receiverlist, inhoud)
    print("Successfully sent email")


def scanfolder2list(input_Scan_folderList, FileTypes, SearchStrings, List):
    filecount = 0
    for top in input_Scan_folderList:
        print("Working in: " + top)
        for root, dirs, files in os.walk(top, topdown=False):
            for fl in files:
                currentFile = os.path.join(root, fl)
                for FileType in FileTypes:
                    status = str.endswith(currentFile, FileType)
                    if str(status) == 'True':
                        for SearchString in SearchStrings:
                            if str(SearchString in currentFile) == 'True':
                                # print str(currentFile)+str(status)
                                List.append(currentFile)
                filecount = filecount + 1


def insertlog(Locbestand, scanmapurl, meting_naam, connection):
    print(scanmapurl[0])
    bestand = Locbestand.split(scanmapurl[0])
    # print bestand
    map_bestand = (scanmapurl[0].split(splittrigger))[1] + '\\' + bestand[1][1:]
    # print map_bestand
    # meting_naam2=meting_naam[:len(bestand[1][1:-4])]
    meting_naam3 = meting_naam.replace('\r', '')
    meting_naam2 = meting_naam3.replace('\n', '')
    # mapweg=(meting_naam2.split('\'))[0]
    # print meting_naam3
    # print meting_naam4
    # print '-'+str(bestand[1][1:-4])+'-'
    # print '-'+str(meting_naam4)+'-'

    # if str(bestand[1][1:-4])==str(meting_naam2):
    if str(bestand[1][1:-4].split('\\')[1]) == str(meting_naam2):
        print(str(bestand[1][1:-4].split('\\')[1]))
        print(str(meting_naam2))
        controle_header = 'bestandsnaam conform header'
        print(controle_header)
    else:
        controle_header = ' bestandsnaam NIET conform header'
        print(controle_header)

    if str(bestand[1][1:-4].split('\\')[0]) == str(meting_naam2).split(' ')[0]:
        print(str(bestand[1][1:-4].split('\\')[0]))
        print(str(meting_naam2).split(' ')[0])
        controle_header = controle_header + ' / bestandsmap conform header'
        print(controle_header)
    else:
        controle_header = controle_header + ' / bestandsmap NIET conform header!'
        print(controle_header)

    fieldinfo = [
        (Locbestand, map_bestand, bestand[1][1:-4], meting_naam2, controle_header, time.strftime('%Y-%m-%d %H:%M:%S'))]
    cursor = connection.cursor()
    # fieldinfo=[(oid,(LocPlanscan),(LocPlanscan[-4:]),str(time.asctime(time.localtime(os.path.getmtime(LocPlanscan)))),int(time.strftime('%Y%m%d',(time.localtime(os.path.getmtime(LocPlanscan))))),time.strftime('%Y-%m-%d %H:%M:%S'))]
    ##str(time.asctime(time.localtime(os.path.getmtime(LocPlanscan)))),int(time.strftime('%Y%m%d',(time.localtime(os.path.getmtime(LocPlanscan)))))
    # query = "INSERT INTO "+PostgisTabelName+ "(oid,LocPlanscan,Type,inleesdatum) VALUES (%s, %s, %s, %s)"
    query = "INSERT INTO " + + "(bestands_url,map_bestand,bestandsnaam,measure_series_name,ctr_header_bestandsnaam,inleesdatum) VALUES (%s,%s,%s,%s,%s,%s)"
    # print query
    cursor.executemany(query, fieldinfo)
    connection.commit()


##if pcname == pcnamen[0]:
##    ConnString=0
##elif pcname ==pcnamen[1]:
##    ConnString=1
##elif pcname ==pcnamen[2]:
##    ConnString=2
##else :
##
### verstuur mail indien script van de juist pc is gestart
##    verstuurmail(mail_hostname,sender,receiverlist,message)

def main():
    outputlist = []
    scanfolder2list(Scan_folder, bestandtypes, zoektext, outputlist)
    connection = psycopg2.connect(PostgisConnString[ConnString])
    cursor = connection.cursor()

    # cursor.execute("DROP TABLE IF EXISTS "+PostgisTabelName+"")
    ##print 'drop table'

    cursor.execute(
        'create table if not exists ' + PostgisTabelName + ' (oid serial,bestands_url character varying,toestel varchar (100),meting_serie_naam varchar (100),rijstrook varchar (5),toestel_zijde varchar (5),markering varchar (5),locatieomschrijving varchar (100),operator_naam varchar (25), kmp numeric (10,4),tijdstip_meting_tekst varchar (25),tijdstip_meting timestamp without time zone, RL int,RLmin int,RLmax int, RLstdDev int, RLprocPass int, marker varchar (100), temperatuur int, vochtigheid int, snelheid numeric(6,2),reference varchar(25), Longitude_start numeric(12,9),Latitude_start numeric(12,9),Longitude_eind numeric(12,9),Latitude_eind numeric(12,9), naam_foto1 varchar(25),naam_foto2 varchar(25), inleesdatum timestamp without time zone);')
    # print 'ceate table ' +str(PostgisTabelName)
    connection.commit()

    # cursor.execute("DROP TABLE IF EXISTS "+PostgisLOGTabel+"")
    ##print 'drop table'

    cursor.execute(
        'create table if not exists ' + PostgisLOGTabel + ' (oid serial,bestands_url character varying,map_bestand character varying,bestandsnaam character varying,measure_series_name character varying,ctr_header_bestandsnaam character varying,inleesdatum timestamp without time zone);')
    # print 'ceate table ' +str(PostgisLOGTabel)
    connection.commit()

    print(sql_delete_fouten)
    cursor.execute(sql_delete_fouten)
    print("gegevens metingen met niet conforme bestands/map/naam met header zijn verwijderd")
    connection.commit()
    print(sql_delete_logfouten)
    cursor.execute(sql_delete_logfouten)
    print("gegevens log met niet conforme bestandsnaam zijn verwijderd")
    connection.commit()

    cursor.execute("SELECT bestands_url  FROM " + PostgisLOGTabel + "")
    listpg = cursor.fetchall()
    print('listpg : ' + str(listpg))
    maxoid = len(listpg)
    teverwerken = len(outputlist) - maxoid
    print(teverwerken)

    if teverwerken == 0:
        print("geen nieuwe bestanden")
        exit
    else:
        # print 'lijstdwgpg'+str(listpgdwg)
        listpg2 = ()
        for n in listpg:
            listpg2 += n
        # oid=maxoid
        # rijstrook=1
        # trigger='RRT'
        print('listpg2 : ' + str(listpg2))
        print(outputlist)

        for file in outputlist:
            print('file ' + str(file))

            # insertlog(file,Scan_folder,meting_serie_naam,connection)
            # file_=str(file.split(Scan_folder[0])[1])
            # file_=str(file.split('\\'))[-2]
            # print file_
            # print 'scanfolder ' + Scan_folder[0]
            # submap=str((Scan_folder[0].split(splittrigger))[1])
            # print 'string  \\'+ str((Scan_folder[0].split(splittrigger))[1])+'\\'+str(file_)
            # file2= '\\'+str((Scan_folder[0].split(splittrigger))[1])+'\\'+str(file_)
            # print file2
            # print os.path.join(submap,file_)
            # print 'file_: ' + str(file_)
            if any(file in s for s in listpg2):  # or trigger in file :
                print('reeds verwerkt ' + str(file))


            else:
                print('lees nieuw bestand in en voeg toe in databank')

                # for lijst in bestandsnaam:
                # bestand= os.path.join(url,lijst)
                bestand = os.path.join(url, file)

                with open(bestand, 'r') as f:
                    data = f.readlines()

                    # print data[0:19]
                    print(data[0].split('\t')[1])
                    toestel = data[0].split('\t')[1]
                    ##                print data[1]
                    print(data[2].split('\t')[1])
                    meting_serie_naam = data[2].split('\t')[1]
                    ##                print data[3]
                    ##                print data[4]
                    ##                print data[5]
                    print(data[6])
                    rijstrook = data[6].split('\t')[1]
                    ##                print data[7]
                    ##                print data[8]
                    ##                print data[9]
                    toestel_zijde = data[9].split('\t')[1]
                    print(data[10])
                    markering = data[10].split('\t')[1]
                    print(data[11])
                    locatieomschrijving = data[11].split('\t')[1]
                    print(data[12])
                    operator = data[12].split('\t')[1]
                    ##                print data[13]
                    ##                print data[14]
                    ##                print data[15]
                    ##                print data[16]
                    ##                print data[17]
                    print('dit zijn de kolomvelden :' + str(data[18]))
                    # split de kolomvelden op tab
                    for line in data[19:]:
                        words = line.split('\t')
                        # print words[0]
                        kmp = words[0].replace(',', '.')
                        # print words[1]
                        tijdstip_meting_tekst = words[1]
                        # print 'tijdstip_meting_tekst: ' +str(tijdstip_meting_tekst)
                        tijdstip_meting2 = words[1].replace('/', '-')
                        tijdstip_meting = datetime.datetime.strptime(tijdstip_meting2, '%d-%m-%Y %H:%M:%S')
                        # print 'tijdstip_meting:' +str(tijdstip_meting)
                        RL = words[2]
                        # print words[3]
                        RLmin = words[3]
                        # print words[4]
                        RLmax = words[4]
                        # print words[5]
                        RLstdDev = words[5]
                        # print words[6]
                        RLprocPass = words[6]
                        # print words[7]
                        marker = words[7]
                        # print words[8]
                        temperatuur = words[8]
                        # print words[9]
                        vochtigheid = words[9]
                        # print words[10]
                        snelheid = words[10].replace(',', '.')
                        # print words[11]
                        reference = words[11]
                        Longitude_start = words[12].replace(',', '.')
                        # print words[12]
                        Latitude_start = words[13].replace(',', '.')
                        # print words[13]
                        Longitude_eind = words[14].replace(',', '.')
                        # print words[14]
                        Latitude_eind = words[15].replace(',', '.')
                        # print words[15]
                        naam_foto1 = words[16]
                        # print words[16]
                        naam_foto2 = words[17]

                        inleesdatum = time.strftime('%Y-%m-%d %H:%M:%S')
                        inleesdag = time.strftime('%Y-%m-%d')
                        meting = [(str(file), toestel, meting_serie_naam, rijstrook, toestel_zijde, markering,
                                   locatieomschrijving, operator, float(kmp), tijdstip_meting_tekst, tijdstip_meting,
                                   RL, RLmin, RLmax, RLstdDev, RLprocPass, marker, temperatuur, vochtigheid, snelheid,
                                   reference, Longitude_start, Latitude_start, Longitude_eind, Latitude_eind,
                                   naam_foto1, naam_foto2, inleesdatum)]
                        query = "INSERT INTO " + PostgisTabelName + " (bestands_url,toestel ,meting_serie_naam,rijstrook ,toestel_zijde ,markering ,locatieomschrijving ,operator_naam,kmp,tijdstip_meting_tekst,tijdstip_meting , RL ,RLmin,RLmax, RLstdDev , RLprocPass , marker , temperatuur, vochtigheid , snelheid ,reference, Longitude_start,Latitude_start ,Longitude_eind ,Latitude_eind , naam_foto1,naam_foto2 , inleesdatum) VALUES (%s, %s ,%s , %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s ,%s , %s, %s, %s, %s, %s, %s, %s, %s, %s , %s, %s, %s, %s)"
                        # print query
                        print(meting)
                        cursor.executemany(query, meting)
                        connection.commit()

                insertlog(file, Scan_folder, meting_serie_naam, connection)

        cursor.execute(sql_count_naamfout)
        foutaantal = cursor.fetchall()
        foutenaantal = '\n'.join(str(x) for x in foutaantal)
        print('fouten ' + str(foutaantal))
        print('foutenaantal ' + str(foutenaantal))
        if '0L' not in foutenaantal:
            print('er zijn fouten gedecteerd')
            cursor.execute(sql_naamfout)
            connection.commit()
            foutrecords = cursor.fetchall()
            foutenlijst = '\n'.join(str(x) for x in foutrecords)
            print(foutenlijst)

        print(sql_create_table)
        # cursor.execute(sql_create_table)
        # connection.commit()
        print(sql_update_insert)
        # cursor.execute(sql_update_insert)
        # connection.commit()
        print(sql_make_line)
        # cursor.execute(sql_make_line)
        # connection.commit()

    ##    current_mxd = arcpy.mapping.MapDocument(os.path.join(ws, mxdname))
    ##    pdf_name = mxdname[:-4] + ".pdf"
    ##    pdfurl=os.path.join(url,pdf_name)
    ##    arcpy.mapping.ExportToPDF(current_mxd, pdfurl)
    # arcpy.env.workspace = ws = mxdmap
    # mxd_list = arcpy.ListFiles("*.mxd")
    # print mxd_list

    ##        for mxd in mxd_list:
    ##            print mxd
    ##            current_mxd = arcpy.mapping.MapDocument(os.path.join(ws, mxd))
    ##            pdf_name = mxd[:-4]+"_"+ meetjaar +"_" +str(inleesdag)+ ".pdf"
    ##
    ##            arcpy.mapping.ExportToPDF(current_mxd, pdf_name)
    ##            pdfurl=os.path.join(mxdmap,pdf_name)
    ##            pdfdesturl=os.path.join(url,pdf_name)
    ##            pdfdesturl2=os.path.join(districtchefmap,pdf_name)
    ##            copybestand(pdfurl,pdfdesturl)
    ##            copybestand(pdfurl,pdfdesturl2)
    ##        del mxd_list

    # copybestand(file,dest_file)
    eindtijd = time.strftime('%Y-%m-%d %H:%M:%S')
    print(eindtijd)
    cursor.execute(sql_list_ingelezenbestanden)
    connection.commit()
    ingelezen = cursor.fetchall()
    print(ingelezen)
    ingelezenlijst = '\n'.join(str(x) for x in ingelezen)
    print(ingelezenlijst)
    connection.close()
    print(ingelezenlijst)


if __name__ == "__main__":
    main()
