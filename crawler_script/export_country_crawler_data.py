import os, sys
sys.path.append( os.path.join( os.path.abspath(os.path.dirname(__file__)) , '../'))
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import math
import json
save_engine = create_engine('mysql+mysqlconnector://%s:%s@%s/%s?charset=utf8' % ("db_xs_gldjc", "xs.gldjc.c0m", "10.125.4.73:3308", "db_xs_new"),pool_recycle=3600, pool_size=15)
crawler_engine = create_engine('mysql+mysqlconnector://%s:%s@%s/%s?charset=utf8' % ("crawlergov", "X2xMGSJjLKC490%", "10.125.4.148:8635", "crawler_gov"),pool_recycle=3600, pool_size=15)
save_Session = sessionmaker(save_engine)
crawler_Session = sessionmaker(crawler_engine)
save_session = save_Session()
crawler_session = crawler_Session()
crawler_count = 187924
page_size = 1000
pages = math.ceil(crawler_count/page_size)
for page in range(pages):
    for one in crawler_session.execute("select * from crawler_content_result_record where project = 'country' limit %s offset %s"%(page_size, page_size*page)):
        print(one['taskid'])
        ent_no = save_session.execute("select ent_no from enterprise_base where ent_no='%s'"%(one['taskid'])).scalar()
        if ent_no:
            try:
                qualifications = json.loads(one['result']).get('qualifications') if json.loads(one['result']).get('qualifications') else list()
                persons = json.loads(one['result']).get('persons') if json.loads(one['result']).get('persons') else list()
                if qualifications:
                    save_session.execute("delete from enterprise_qualification where enterprise_no='%s'"%(ent_no))
                if persons:
                    save_session.execute("delete from enterprise_registered_person where enterprise_no='%s'"%(ent_no))
                for qualification in qualifications:
                    save_session.execute("insert into enterprise_qualification(enterprise_no,category,name,credentials_info,credentials_no,issued_org,issued_date,deadline,updated_date,created_date) values('%s','%s','%s','%s','%s','%s','%s','%s',now(),now())"%(ent_no,qualification['type'],qualification['name'],qualification['code'],qualification['code'],qualification['gov'],qualification['start_date'],qualification['end_date']))
                serial_number = 1
                for person in persons:
                    register_major = person.get('register_major')
                    register_type = person.get('register_type')
                    register_code = person.get('register_code')
                    if not register_major:
                        for q in person.get('qualifications',[]):
                            if q.get('register_major'):
                                register_major = q.get('register_major')
                            if q.get('register_type'):
                                register_type = person.get('register_type')
                            if q.get('register_code'):
                                register_code = person.get('register_code')
                    save_session.execute("insert into enterprise_registered_person(enterprise_no,name,serial_number,major,category,identification_number,registration_number,updated_date,created_date) values('%s','%s','%s','%s','%s','%s','%s',now(),now())"%(ent_no,person['name'],serial_number,register_major,register_type,person['certificate_code'],register_code))
                    serial_number += 1
                save_session.commit()
            except Exception as e:
                import pdb;pdb.set_trace()
                save_session.rollback()
                raise e
save_session.close()
crawler_session.close()
print('finish')
