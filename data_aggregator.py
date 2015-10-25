from config import *
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import sessionmaker


crawler_database = 'mysql+pymysql://%s:%s@%s:%s' % (CRAWLER_DB_USERNAME, CRAWLER_DB_PASSWORD,
                                                    CRAWLER_DB_HOSTNAME, CRAWLER_DB_PORT)

viral_database = 'mysql+pymysql://%s:%s@%s:%s' % (VIRAL_DB_USERNAME, VIRAL_DB_PASSWORD,
                                                  VIRAL_DB_HOSTNAME, VIRAL_DB_PORT)

crawler_engine = create_engine(crawler_database, echo=True)
viral_engine = create_engine(viral_database, echo=True)

crawler_Session = sessionmaker(bind=crawler_engine)
viral_Session = sessionmaker(bind=viral_engine)

crawler_session = crawler_Session()
viral_session = viral_Session()


def insert_into_board(data_list, timestamp=None):
    pass


def insert_into_board_current_state(data_list, timestamp=None):
    pass


def insert_into_board_manager(data_list, timestamp=None):
    pass


def insert_into_like_counts(data_list, timestamp=None):
    pass


def insert_into_talk_about_counts(data_list, timestamp=None):
    pass


def insert_into_share_counts(data_list, timestamp=None):
    pass


def insert_into_comment_counts(data_list, timestamp=None):
    pass


def insert_into_article_counts(data_list, timestamp=None):
    pass


def aggregate_like_count(fb_id, start_date, end_date):
    pass


def aggregate_talk_about_count(fb_id, start_date, end_date):
    pass


def aggregate_article_count(fb_id, start_date, end_date):
    pass


def aggregate_share_count(fb_id, start_date, end_date):
    pass


def aggregate_comment_count(fb_id, start_date, end_date):
    pass


INSERT_TABLE_DICT = {"board": insert_into_board,
                     "board_current_state": insert_into_board_current_state,
                     "likeCounts": insert_into_like_counts,
                     "talkAboutCounts": insert_into_talk_about_counts,
                     "shareCounts": insert_into_share_counts,
                     "commentCounts": insert_into_comment_counts,
                     "articleCounts": insert_into_article_counts,
                     "board_managers": insert_into_board_manager}

FUNCTION_DICT = {"likeCounts": aggregate_like_count,
                 "talkAboutCounts": aggregate_talk_about_count,
                 "articleCounts": aggregate_article_count,
                 "shareCounts": aggregate_share_count,
                 "commentCounts": aggregate_comment_count}


def insert_function(attribute_type, data_list, timestamp=None):
    if INSERT_TABLE_DICT.has_key(attribute_type):
        strategy = INSERT_TABLE_DICT[attribute_type]
        return strategy(data_list, timestamp)


def aggregate_function(attribute_type, fb_id, start_date, end_date):
    if FUNCTION_DICT.has_key(attribute_type):
        strategy = FUNCTION_DICT[attribute_type]
        return strategy(fb_id, start_date, end_date)


if __name__ == "__main__":
    insert_function("board_managers",
                    [{"fb_id":"0001", "user_id": "user0001"},{"fb_id":"0002", "user_id": "user0002"}],
                    timestamp="2015-10-25-14:00:15")
    pass