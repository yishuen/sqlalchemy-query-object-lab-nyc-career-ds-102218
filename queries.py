from sqlalchemy import create_engine, func, or_
from seed import Company
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///dow_jones.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def return_apple():
    return session.query(Company).filter_by(company = 'Apple').first()

def return_disneys_industry():
    disney = session.query(Company).filter(Company.company=='Walt Disney').first()
    return disney.industry

def return_list_of_company_objects_ordered_alphabetically_by_symbol():
    return session.query(Company).order_by(Company.symbol).all()

def return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending():
    tech_companies = session.query(Company).filter_by(industry = 'Technology').order_by(Company.enterprise_value.desc())
    return list(map(lambda co: {'company': co.company, 'EV': co.enterprise_value}, tech_companies))

def return_list_of_consumer_products_companies_with_EV_above_225():
    cp_companies = session.query(Company).filter(Company.industry == 'Consumer products', Company.enterprise_value > 225).all()
    return list(map(lambda co: {'name': co.company}, cp_companies))

def return_conglomerates_and_pharmaceutical_companies():
    cp_companies = session.query(Company).filter(or_(Company.industry == 'Conglomerate', Company.industry == 'Pharmaceuticals'))
    return list(map(lambda co: co.company, cp_companies))

def avg_EV_of_dow_companies():
    return session.query(func.avg(Company.enterprise_value)).first()

def return_industry_and_its_total_EV():
    return session.query(Company.industry, func.sum(Company.enterprise_value)).group_by(Company.industry).all()
