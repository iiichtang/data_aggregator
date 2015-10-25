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
    # insert / update the board information in viralrank
    pass


def insert_into_board_current_state(data_list, timestamp=None):
    # insert / update the current board information in viralrank
    pass


def insert_into_board_manager(data_list, timestamp=None):
    # insert / update the manager counts in viralrank
    pass


def insert_into_board_category(data_list, timestamp=None):
    # insert the categories of boards
    pass


def insert_into_like_counts(data_list, timestamp=None):
    # insert the like counts of boards
    pass


def insert_into_talk_about_counts(data_list, timestamp=None):
    # insert the talk-about counts of boards
    pass


def insert_into_share_counts(data_list, timestamp=None):
    # insert the share counts of boards
    pass


def insert_into_comment_counts(data_list, timestamp=None):
    # insert the comment counts of boards
    pass


def insert_into_article_counts(data_list, timestamp=None):
    # insert the article count of boards
    pass


def insert_into_overall_statistics(data_list, timestamp=None):
    # insert / update the overall_statistics of viralrank
    pass


def get_board(board_id, start_date, end_date):
    # fetch the board information in crawler
    pass


def aggregate_board_current_state(board_id, start_date, end_date):
    # aggregate the current state of boards
    pass


def get_board_managers(board_id, start_date, end_date):
    # fetch the manager count of boards in crawler
    pass


def aggregate_like_count(board_id, start_date, end_date):
    # aggregate the like count of boards
    pass


def aggregate_talk_about_count(board_id, start_date, end_date):
    # aggregate the talk about count of boards
    pass


def aggregate_article_count(board_id, start_date, end_date):
    # aggregate the article count of boards
    pass


def aggregate_share_count(board_id, start_date, end_date):
    # aggregate the share count of boards
    pass


def aggregate_comment_count(board_id, start_date, end_date):
    # aggregate the comment count of boards
    pass


def aggregate_overall_statistic(board_id=None, start_date=None, end_date=None):
    # aggregate the overall statistics
    pass


INSERT_TABLE_DICT = {"board": insert_into_board,
                     "board_current_state": insert_into_board_current_state,
                     "board_managers": insert_into_board_manager,
                     "likeCounts": insert_into_like_counts,
                     "talkAboutCounts": insert_into_talk_about_counts,
                     "shareCounts": insert_into_share_counts,
                     "commentCounts": insert_into_comment_counts,
                     "articleCounts": insert_into_article_counts,
                     "overall_statistics": insert_into_overall_statistics}

FUNCTION_DICT = {"board": get_board,
                 "board_current_state": aggregate_board_current_state,
                 "board_managers": get_board_managers,
                 "likeCounts": aggregate_like_count,
                 "talkAboutCounts": aggregate_talk_about_count,
                 "articleCounts": aggregate_article_count,
                 "shareCounts": aggregate_share_count,
                 "commentCounts": aggregate_comment_count,
                 "overall_statistics": aggregate_overall_statistic}


def insert_function(attribute_type, data_list, timestamp=None):
    if INSERT_TABLE_DICT.has_key(attribute_type):
        strategy = INSERT_TABLE_DICT[attribute_type]
        return strategy(data_list, timestamp)


def aggregate_function(attribute_type, board_id, start_date, end_date):
    if FUNCTION_DICT.has_key(attribute_type):
        strategy = FUNCTION_DICT[attribute_type]
        return strategy(board_id, start_date, end_date)


if __name__ == "__main__":
    insert_function("board_managers",
                    [{"board_id": "0001", "count": "15"}, {"board_id": "0002", "count": "59"}],
                    timestamp="2015-10-25-14:00:15")

    aggregate_function("likeCounts", "0001", "2015-01-01", "2015-10-25")
    pass
