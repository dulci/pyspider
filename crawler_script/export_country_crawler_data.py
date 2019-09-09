import os, sys
sys.path.append( os.path.join( os.path.abspath(os.path.dirname(__file__)) , '../'))
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import math
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
        ent_no = save_session.execute("select ent_no from enterprise_base where ent_name='%s'"%(one['company_name'])).scalar()
        if ent_no:
            try:
                save_session.execute("delete from enterprise_qualification where enterprise_no='%s'"%(ent_no))
                save_session.execute("delete from enterprise_registered_person where enterprise_no='%s'"%(ent_no))
                for qualification in one['result'].get('qualifications'):
                    save_session.execute("insert into enterprise_qualification(enterprise_no,category,name,credentials_info,credentials_no,issued_org,issued_date,deadline) values('%s','%s','%s','%s','%s','%s','%s','%s')"%(ent_no,qualification['type'],qualification['name'],qualification['code'],qualification['code'],qualification['gov'],qualification['start_date'],qualification['end_date']))
                serial_number = 1
                for person in one['result'].get('persons'):
                    save_session.execute("insert into enterprise_registered_person(enterprise_no,name,serial_number,major,category,identification_number,registration_number) values('%s')"%(ent_no,person['name'],serial_number,person['register_major'],person['register_type'],person['certificate_code'],person['register_code']))
                    serial_number += 1
                save_session.commit()
            except:
                save_session.rollback()
save_session.close()
crawler_session.close()