from PostGISConnector import PostGISConnector


def create_analysis_tables(connector: PostGISConnector, report_year: int, beheersegmenten_year: int):
    connection = connector.get_connection()

    metingen_per_wegcategorie_en_provincie(connection=connection, report_year=report_year,
                                           beheersegmenten_year=beheersegmenten_year)
    metingen_per_district_per_klasse(connection=connection, report_year=report_year)
    metingen_procent_per_prov_m(connection=connection, report_year=report_year)
    metingen_per_district_per_klasse_H(connection=connection, report_year=report_year)
    metingen_per_district_per_klasse_P(connection=connection, report_year=report_year)
    metingen_procent_per_prov_vl_H(connection=connection, report_year=report_year)
    metingen_procent_per_prov_vl_H_m1(connection=connection, report_year=report_year)
    metingen_procent_per_prov_vl_H_m2(connection=connection, report_year=report_year)
    metingen_procent_per_prov_vl_P(connection=connection, report_year=report_year)
    metingen_procent_per_prov_vl_P_m1(connection=connection, report_year=report_year)
    metingen_procent_per_prov_vl_P_m2(connection=connection, report_year=report_year)
    metingen_procent_per_prov_M1(connection=connection, report_year=report_year)
    metingen_procent_per_prov_M2(connection=connection, report_year=report_year)
    metingen_procent_per_prov_hoofdweg(connection=connection, report_year=report_year)
    metingen_procent_per_prov_hoofdweg_M1(connection=connection, report_year=report_year)
    metingen_procent_per_prov_hoofdweg_M2(connection=connection, report_year=report_year)
    metingen_procent_per_prov_P1P2weg(connection=connection, report_year=report_year)
    metingen_procent_per_prov_P1P2weg_M1(connection=connection, report_year=report_year)
    metingen_procent_per_prov_P1P2weg_M2(connection=connection, report_year=report_year)

    connection.commit()


def metingen_per_wegcategorie_en_provincie(connection, report_year: int, beheersegmenten_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_per_district_wegcat;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_per_district_wegcat as
SELECT ttmr.toestel, ttmr.ident8, kmp, tijdstip_meting,replace(meting_serie_naam,E'\n','') as meting_serie_naam
	,markering,locatieomschrijving, rl, rlmin, rlmax, rlstddev, rlprocpass,snelheid,marker
	,latitude_start, longitude_start, latitude_eind, longitude_eind, segmenten.district, segmenten.provincie, segmenten.wegcategorie, oid, geoml72
FROM ttw.ttw_t_mobiele_retroreflectometer{report_year} ttmr
	JOIN ttw.ttw.gt_beheersegmenten_{beheersegmenten_year} segmenten ON ttmr.ident8 = segmenten.ident8
		AND kmp BETWEEN segmenten.beginpositie  AND segmenten.eindpositie
	--where eigenbeheer=true --and marker not like '%Obstakel%'
	--and rl<>0
ORDER by ttmr.ident8, ttmr.kmp;"""
    cursor.execute(sql)


def metingen_per_district_per_klasse(connection, report_year: int):
    # Maak eind tabel (alle wegen) met de bijbehorende klassen
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse AS
SELECT ident8,kmp,tijdstip_meting,meting_serie_naam,markering,locatieomschrijving, rl, rlmin, rlmax, rlstddev, rlprocpass, 
   snelheid,latitude_start, longitude_start, latitude_eind, 
   longitude_eind, district, 
   provincie,wegcategorie,
   ttw_t_retroreflectometer_klasse.klasse,
   ttw_t_retroreflectometer_klasse.luminatie,
   ttw_t_retroreflectometer_klasse.performatieklasse,
   ttw_t_retroreflectometer_klasse.beoordeling,
   ttw_t_retroreflectometer_klasse.actie,
      case
	WHEN right (ident8,1)='1' THEN 1000
	WHEN right (ident8,1)='2' THEN -1000
	END as offset_1000
FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_wegcat
	JOIN ttw.ttw_t_retroreflectometer_klasse ON rl BETWEEN ttw_t_retroreflectometer_klasse.l_min and ttw_t_retroreflectometer_klasse.l_max
WHERE rl<>0; --nulwaarden niet in rekening gebracht"""
    cursor.execute(sql)


def metingen_procent_per_prov_m(connection, report_year: int):
    # Maak eind tabel (alle wegen) met de bijbehorende klassen
    cursor = connection.cursor()
    sql = f"""    
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_m;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_m AS
SELECT date_part('year',tijdstip_meting)as Jaar,--meting_serie_naam,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*) FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='Antwerpen')*100.0,1) as Percentage_metingen
 , provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
--JOIN provincie ON provincie=provincie.provcode
where provincie='Antwerpen'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*) FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='Vlaams-Brabant')*100.0,1) as Percentage_metingen
 ,provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
  --JOIN provincie ON provincie=domain_provincie.provcode
where provincie='Vlaams-Brabant'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*) FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='West-Vlaanderen')*100.0,1) as Percentage_metingen
 , provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='West-Vlaanderen'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*) FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='Oost-Vlaanderen')*100.0,1) as Percentage_metingen
 , provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Oost-Vlaanderen'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*) FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='Limburg')*100.0,1) as Percentage_metingen
, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Limburg'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
order by provincie,klasse;"""
    cursor.execute(sql)


def metingen_per_district_per_klasse_H(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H AS
SELECT ident8,kmp,tijdstip_meting,meting_serie_naam,markering,locatieomschrijving, rl, rlmin, rlmax, rlstddev, rlprocpass, 
   snelheid,latitude_start, longitude_start, latitude_eind, 
   longitude_eind, district, 
   provincie,wegcategorie,
   ttw_t_retroreflectometer_klasse.klasse,
   ttw_t_retroreflectometer_klasse.luminatie,
   ttw_t_retroreflectometer_klasse.performatieklasse,
   ttw_t_retroreflectometer_klasse.beoordeling,
   ttw_t_retroreflectometer_klasse.actie,
   case WHEN right (ident8,1)='1' THEN 1000
        WHEN right (ident8,1)='2' THEN -1000
        END as offset_1000
FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_wegcat
JOIN ttw.ttw_t_retroreflectometer_klasse ON rl
 BETWEEN ttw_t_retroreflectometer_klasse.l_min and ttw_t_retroreflectometer_klasse.l_max
where wegcategorie like 'H%' and rl <> 0;"""
    cursor.execute(sql)


def metingen_per_district_per_klasse_P(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P AS
SELECT ident8,kmp,tijdstip_meting,meting_serie_naam,markering,locatieomschrijving, rl, rlmin, rlmax, rlstddev, rlprocpass, 
       snelheid,latitude_start, longitude_start, latitude_eind, 
       longitude_eind, district, 
       provincie,wegcategorie,
       ttw_t_retroreflectometer_klasse_p.klasse,
       ttw_t_retroreflectometer_klasse_p.luminatie,
       ttw_t_retroreflectometer_klasse_p.performatieklasse,
       ttw_t_retroreflectometer_klasse_p.beoordeling,
       ttw_t_retroreflectometer_klasse_p.actie,
	CASE WHEN right (ident8,1)='1' THEN 1000
		WHEN right (ident8,1)='2' THEN -1000
		END as offset_1000
FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_wegcat
JOIN ttw.ttw_t_retroreflectometer_klasse_p ON rl
 BETWEEN ttw_t_retroreflectometer_klasse_p.l_min and ttw_t_retroreflectometer_klasse_p.l_max
where wegcategorie like 'P%' and rl<>0;"""
    cursor.execute(sql)


def metingen_procent_per_prov_vl_H(connection, report_year: int):
    # --vlaanderen H weg
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_H;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_H AS
SELECT date_part('year',tijdstip_meting)as Jaar,round(count(rl)*1.0/(SELECT count(*) FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H)*100.0,1) as Percentage_metingen
 ,'M1_M2'::character varying(5)as markering, 'Vlaanderen'::character varying(25) as gebied, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
group by date_part('year',tijdstip_meting),gebied, klasse , luminatie, performatieklasse, beoordeling,actie
order by klasse;"""
    cursor.execute(sql)


def metingen_procent_per_prov_vl_H_m1(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_H_m1;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_H_m1 AS
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*) FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, 'Vlaanderen'as gebied, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
where  markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,gebied, klasse , luminatie, performatieklasse, beoordeling,actie
order by klasse;"""
    cursor.execute(sql)


def metingen_procent_per_prov_vl_H_m2(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_H_m2;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_H_m2 AS
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*) FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, 'Vlaanderen'as gebied, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
where  markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,gebied, klasse , luminatie, performatieklasse, beoordeling,actie
order by klasse;"""
    cursor.execute(sql)


def metingen_procent_per_prov_vl_P(connection, report_year: int):
    # --vlaanderen P weg
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_P;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_P AS
SELECT date_part('year',tijdstip_meting)as Jaar,round(count(rl)*1.0/(SELECT count(*) FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P)*100.0,1) as Percentage_metingen
 ,'M1_M2'::character varying(5)as markering, 'Vlaanderen'::character varying(25) as gebied, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
group by date_part('year',tijdstip_meting),gebied, klasse , luminatie, performatieklasse, beoordeling,actie
order by klasse;"""
    cursor.execute(sql)
    

def metingen_procent_per_prov_vl_P_m1(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_P_m1;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_P_m1 AS
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*) FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, 'Vlaanderen'as gebied, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
where  markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,gebied, klasse , luminatie, performatieklasse, beoordeling,actie
order by klasse;"""
    cursor.execute(sql)


def metingen_procent_per_prov_vl_P_m2(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_P_m2;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_vl_P_m2 AS
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*) FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, 'Vlaanderen'as gebied, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
where  markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,gebied, klasse , luminatie, performatieklasse, beoordeling,actie
order by klasse;"""
    cursor.execute(sql)


def metingen_procent_per_prov_M1(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_M1;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_M1 AS
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='Antwerpen'and markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
--JOIN provincie ON provincie=provincie.provcode
where provincie='Antwerpen'and markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='Vlaams-Brabant'and markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering,provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
  --JOIN provincie ON provincie=domain_provincie.provcode
where provincie='Vlaams-Brabant'and markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='West-Vlaanderen'and markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='West-Vlaanderen'and markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='Oost-Vlaanderen'and markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Oost-Vlaanderen'and markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='Limburg'and markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Limburg'and markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
order by provincie,klasse;
"""
    cursor.execute(sql)


def metingen_procent_per_prov_M2(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_M2;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_M2 AS
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='Antwerpen'and markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
--JOIN provincie ON provincie=provincie.provcode
where provincie='Antwerpen'and markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='Vlaams-Brabant'and markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering,provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
  --JOIN provincie ON provincie=domain_provincie.provcode
where provincie='Vlaams-Brabant'and markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='West-Vlaanderen'and markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='West-Vlaanderen'and markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='Oost-Vlaanderen'and markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Oost-Vlaanderen'and markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse where provincie='Limburg'and markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Limburg'and markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
order by provincie,klasse;
"""
    cursor.execute(sql)


def metingen_procent_per_prov_hoofdweg(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_hoofdweg;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_hoofdweg AS
--M1 en M2 samen berekenen
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='Antwerpen')*100.0,1) as Percentage_metingen
 , provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
--JOIN provincie ON provincie=provincie.provcode
where provincie='Antwerpen'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='Vlaams-Brabant')*100.0,1) as Percentage_metingen
 ,provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
  --JOIN provincie ON provincie=domain_provincie.provcode
where provincie='Vlaams-Brabant'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='West-Vlaanderen')*100.0,1) as Percentage_metingen
 , provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='West-Vlaanderen'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='Oost-Vlaanderen')*100.0,1) as Percentage_metingen
 , provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Oost-Vlaanderen'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='Limburg')*100.0,1) as Percentage_metingen
, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Limburg'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
order by provincie,klasse;
"""
    cursor.execute(sql)


def metingen_procent_per_prov_hoofdweg_M1(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_hoofdweg_M1;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_hoofdweg_M1 AS
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='Antwerpen'and markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
--JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Antwerpen'and markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='Vlaams-Brabant'and markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Vlaams-Brabant'and markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='West-Vlaanderen'and markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='West-Vlaanderen'and markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='Oost-Vlaanderen'and markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Oost-Vlaanderen'and markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='Limburg'and markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Limburg'and markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
order by provincie,klasse;
"""
    cursor.execute(sql)


def metingen_procent_per_prov_hoofdweg_M2(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_hoofdweg_M2;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_hoofdweg_M2 AS
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='Antwerpen'and markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
--JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Antwerpen'and markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='Vlaams-Brabant'and markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Vlaams-Brabant'and markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='West-Vlaanderen'and markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='West-Vlaanderen'and markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='Oost-Vlaanderen'and markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Oost-Vlaanderen'and markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H where provincie='Limburg'and markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_H
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Limburg'and markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
order by provincie,markering,klasse;
"""
    cursor.execute(sql)


def metingen_procent_per_prov_P1P2weg(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_P1P2weg;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_P1P2weg AS

--M1 en M2 samen berekenen
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='Antwerpen')*100.0,1) as Percentage_metingen
 , provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
--JOIN provincie ON provincie=provincie.provcode
where provincie='Antwerpen'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='Vlaams-Brabant')*100.0,1) as Percentage_metingen
 ,provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
  --JOIN provincie ON provincie=domain_provincie.provcode
where provincie='Vlaams-Brabant'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='West-Vlaanderen')*100.0,1) as Percentage_metingen
 , provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='West-Vlaanderen'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='Oost-Vlaanderen')*100.0,1) as Percentage_metingen
 , provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Oost-Vlaanderen'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='Limburg')*100.0,1) as Percentage_metingen
, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Limburg'
group by date_part('year',tijdstip_meting),provincie, klasse , luminatie, performatieklasse, beoordeling,actie
order by provincie,klasse;
"""
    cursor.execute(sql)


def metingen_procent_per_prov_P1P2weg_M1(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_P1P2weg_M1;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_P1P2weg_M1 AS
--M1
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='Antwerpen'and markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
--JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Antwerpen'and markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='Vlaams-Brabant'and markering like 'M1%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Vlaams-Brabant'and markering like 'M1%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='West-Vlaanderen'and markering like 'M1%')*100.0,1) as Percentage_metingen
  ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='West-Vlaanderen'and markering like 'M1%'
group by date_part('year',tijdstip_meting) ,markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='Oost-Vlaanderen'and markering like 'M1%')*100.0,1) as Percentage_metingen
  ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Oost-Vlaanderen'and markering like 'M1%'
group by date_part('year',tijdstip_meting) ,markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='Limburg'and markering like 'M1%')*100.0,1) as Percentage_metingen
  ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Limburg'and markering like 'M1%'
group by date_part('year',tijdstip_meting) ,markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
order by provincie,markering,klasse;
"""
    cursor.execute(sql)


def metingen_procent_per_prov_P1P2weg_M2(connection, report_year: int):
    cursor = connection.cursor()
    sql = f"""
DROP TABLE IF EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_P1P2weg_M2;
CREATE TABLE IF NOT EXISTS ttw.ttw_t_retroreflectometingen_{report_year}_procent_per_prov_P1P2weg_M2 AS
--M2
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='Antwerpen'and markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
--JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Antwerpen'and markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='Vlaams-Brabant'and markering like 'M2%')*100.0,1) as Percentage_metingen
 ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Vlaams-Brabant'and markering like 'M2%'
group by date_part('year',tijdstip_meting),markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='West-Vlaanderen'and markering like 'M2%')*100.0,1) as Percentage_metingen
  ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='West-Vlaanderen'and markering like 'M2%'
group by date_part('year',tijdstip_meting) ,markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='Oost-Vlaanderen'and markering like 'M2%')*100.0,1) as Percentage_metingen
  ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Oost-Vlaanderen'and markering like 'M2%'
group by date_part('year',tijdstip_meting) ,markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
UNION
SELECT date_part('year',tijdstip_meting)as Jaar,--'ja' as nulwaarden_aanwezig ,
round(count(rl)*1.0/(SELECT count(*)FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P where provincie='Limburg'and markering like 'M2%')*100.0,1) as Percentage_metingen
  ,markering, provincie, klasse, luminatie, performatieklasse, beoordeling,actie
  FROM ttw.ttw_t_retroreflectometingen_{report_year}_per_district_per_klasse_P
  --JOIN domain_provincie ON provincie=domain_provincie.provcode
where provincie='Limburg'and markering like 'M2%'
group by date_part('year',tijdstip_meting) ,markering,provincie, klasse , luminatie, performatieklasse, beoordeling,actie
order by provincie,markering,klasse;
"""
    cursor.execute(sql)

