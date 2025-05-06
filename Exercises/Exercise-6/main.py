import logging
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window
from typing import Optional, Tuple
from datetime import timedelta
import zipfile
import io
import csv
from glob import glob # Để tìm file zip

# --- Cấu hình Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Hằng số và Đường dẫn ---
DATA_DIR = Path("data")
REPORTS_DIR = Path("reports")
# Sử dụng glob pattern trực tiếp thay vì tạo path object trước
INPUT_FILES_PATTERN = "data/*.zip"

# --- Khởi tạo SparkSession ---
def create_spark_session(app_name="Exercise6"):
    """Khởi tạo và trả về một SparkSession."""
    logger.info(f"Đang khởi tạo SparkSession: {app_name}")
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        logger.info("SparkSession đã được tạo thành công.")
        # Đặt cấu hình để xử lý timestamp đúng định dạng khi cast từ string
        spark.conf.set("spark.sql.datetime.java8API.enabled", "true") # Cần thiết cho một số xử lý date/time
        return spark
    except Exception as e:
        logger.error(f"Lỗi khi tạo SparkSession: {e}", exc_info=True)
        raise

# --- Định nghĩa Schema ---
# Giữ nguyên schema đã đúng từ trước
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("start_time", TimestampType(), True), # Sẽ đọc là string rồi cast
    StructField("end_time", TimestampType(), True),   # Sẽ đọc là string rồi cast
    StructField("bikeid", IntegerType(), True),
    StructField("tripduration", DoubleType(), True), # Có thể cần cast từ string
    StructField("from_station_id", IntegerType(), True), # Có thể cần cast từ string
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),   # Có thể cần cast từ string
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True),  # Có thể cần cast từ string
])
# Định dạng timestamp mong đợi trong file CSV
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

# --- Các Hàm Xử lý ---

def read_data(spark: SparkSession, path_pattern: str, schema: StructType) -> Optional[DataFrame]:
    """Đọc dữ liệu từ các file CSV bên trong các file ZIP vào Spark DataFrame."""
    logger.info(f"Đang tìm file ZIP tại: {path_pattern}")
    zip_files = glob(path_pattern)

    if not zip_files:
        logger.error(f"Không tìm thấy file ZIP nào khớp với mẫu: {path_pattern}")
        return None

    all_lines_rdd = spark.sparkContext.emptyRDD()
    expected_columns = len(schema.fields)

    logger.info(f"Tìm thấy các file ZIP: {zip_files}")

    for zip_file_path in zip_files:
        logger.info(f"Đang xử lý file ZIP: {zip_file_path}")
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zf:
                csv_files_in_zip = [name for name in zf.namelist() if name.lower().endswith('.csv')]
                if not csv_files_in_zip:
                    logger.warning(f"Không tìm thấy file CSV nào trong: {zip_file_path}")
                    continue

                csv_file_name = csv_files_in_zip[0] # Giả sử chỉ có 1 file CSV cần đọc
                logger.info(f"Đang đọc file CSV '{csv_file_name}' từ bên trong '{zip_file_path}'")

                with zf.open(csv_file_name, 'r') as csv_file:
                    text_stream = io.TextIOWrapper(csv_file, encoding='utf-8', errors='ignore')
                    header_line = next(text_stream).strip() # Đọc và bỏ qua header
                    logger.debug(f"Header CSV được bỏ qua: {header_line}")
                    # Đọc các dòng còn lại, loại bỏ khoảng trắng thừa
                    lines = [line.strip() for line in text_stream if line.strip()]

                    if not lines:
                       logger.warning(f"File CSV '{csv_file_name}' trong '{zip_file_path}' rỗng (sau header).")
                       continue

                    current_rdd = spark.sparkContext.parallelize(lines)
                    all_lines_rdd = all_lines_rdd.union(current_rdd)
                    # Không nên count ở đây vì có thể chậm với nhiều file
                    # logger.info(f"Đã thêm ước tính dòng từ '{csv_file_name}' vào RDD.")

        except zipfile.BadZipFile:
            logger.error(f"File không hợp lệ hoặc không phải file ZIP: {zip_file_path}")
        except Exception as e:
            logger.error(f"Lỗi khi xử lý file ZIP {zip_file_path}: {e}", exc_info=True)

    if all_lines_rdd.isEmpty():
        logger.error("Không đọc được dữ liệu dòng nào từ các file CSV trong các file ZIP.")
        return None

    logger.info("Bắt đầu chuyển đổi RDD thành DataFrame và áp dụng schema...")

    # Hàm parse dùng thư viện csv của Python để an toàn hơn split(',')
    def parse_csv_line(line: str):
        try:
            # Sử dụng csv.reader để xử lý dấu phẩy trong ngoặc kép
            reader = csv.reader([line]) # Wrap dòng vào list để reader hoạt động
            parsed_list = next(reader)
            # Trả về list các string hoặc None nếu parse lỗi/số cột sai
            return parsed_list if len(parsed_list) == expected_columns else None
        except Exception as e:
            # Ghi log lỗi parse nếu cần, nhưng trả về None để lọc
            # logger.debug(f"Lỗi parse dòng: {line} - {e}")
            return None

    # Parse RDD và lọc bỏ các dòng parse lỗi (trả về None)
    parsed_rdd = all_lines_rdd.map(parse_csv_line).filter(lambda x: x is not None)

    if parsed_rdd.isEmpty():
        logger.error("Không có dòng nào được parse thành công theo đúng số cột.")
        return None

    # Tạo DataFrame ban đầu với tất cả các cột là StringType
    # Sử dụng tên cột từ schema để dễ dàng cast sau này
    string_df = parsed_rdd.toDF([field.name for field in schema.fields])
    logger.info("Đã tạo DataFrame ban đầu với các cột kiểu string.")

    # Cast các cột về đúng kiểu dữ liệu theo schema
    select_exprs = []
    for field in schema.fields:
        col_name = field.name
        target_type = field.dataType

        if isinstance(target_type, TimestampType):
            # Xử lý timestamp đặc biệt với format
            expr = F.to_timestamp(F.col(col_name), TIMESTAMP_FORMAT).alias(col_name)
        else:
            # Cast các kiểu khác
            expr = F.col(col_name).cast(target_type).alias(col_name)
        select_exprs.append(expr)

    try:
        df = string_df.select(select_exprs)
        # Thực hiện một action nhỏ để kiểm tra lỗi cast và đếm số dòng hợp lệ
        final_count = df.persist().count() # Persist để các bước sau nhanh hơn
        logger.info(f"Tạo DataFrame thành công với schema. Tổng số bản ghi hợp lệ: {final_count}")
        if final_count == 0:
             logger.warning("DataFrame cuối cùng rỗng sau khi áp dụng schema.")
             df.unpersist()
             return None
        return df
    except Exception as e:
        logger.error(f"Lỗi khi cast kiểu dữ liệu hoặc áp dụng schema cuối cùng: {e}", exc_info=True)
        if 'df' in locals() and df.is_cached:
            df.unpersist()
        return None


# --- Các hàm tính toán (Giữ nguyên như code trước của bạn) ---

def calculate_average_duration_per_day(df: DataFrame) -> Optional[DataFrame]:
    """Câu 1: Tính thời gian chuyến đi trung bình mỗi ngày."""
    logger.info("Câu 1: Tính thời gian chuyến đi trung bình mỗi ngày...")
    try:
        # Đảm bảo cột tripduration là số
        df_filtered = df.filter(F.col("tripduration").isNotNull() & F.col("start_time").isNotNull())
        if df_filtered.rdd.isEmpty():
             logger.warning("Câu 1: Không có dữ liệu hợp lệ (tripduration, start_time) để tính.")
             return None

        df_with_date = df_filtered.withColumn("start_date", F.to_date(F.col("start_time")))
        avg_duration_df = df_with_date.groupBy("start_date") \
            .agg(F.avg("tripduration").alias("average_duration_seconds")) \
            .orderBy("start_date")
        return avg_duration_df
    except Exception as e:
        logger.error(f"Lỗi khi tính câu 1: {e}", exc_info=True)
        return None

def count_trips_per_day(df: DataFrame) -> Optional[DataFrame]:
    """Câu 2: Đếm số chuyến đi mỗi ngày."""
    logger.info("Câu 2: Đếm số chuyến đi mỗi ngày...")
    try:
        df_filtered = df.filter(F.col("start_time").isNotNull())
        if df_filtered.rdd.isEmpty():
             logger.warning("Câu 2: Không có dữ liệu start_time hợp lệ để tính.")
             return None

        df_with_date = df_filtered.withColumn("start_date", F.to_date(F.col("start_time")))
        trip_counts_df = df_with_date.groupBy("start_date") \
            .count() \
            .withColumnRenamed("count", "total_trips") \
            .orderBy("start_date")
        return trip_counts_df
    except Exception as e:
        logger.error(f"Lỗi khi tính câu 2: {e}", exc_info=True)
        return None

def most_popular_start_station_per_month(df: DataFrame) -> Optional[DataFrame]:
    """Câu 3: Tìm trạm xuất phát phổ biến nhất mỗi tháng."""
    logger.info("Câu 3: Tìm trạm xuất phát phổ biến nhất mỗi tháng...")
    try:
        df_filtered = df.filter(F.col("start_time").isNotNull() & F.col("from_station_id").isNotNull() & F.col("from_station_name").isNotNull())
        if df_filtered.rdd.isEmpty():
             logger.warning("Câu 3: Không có dữ liệu hợp lệ (start_time, from_station) để tính.")
             return None

        df_with_month = df_filtered.withColumn("start_month", F.date_format(F.col("start_time"), "yyyy-MM"))
        station_counts = df_with_month.groupBy("start_month", "from_station_id", "from_station_name") \
            .count()
        window_spec = Window.partitionBy("start_month").orderBy(F.col("count").desc())
        ranked_stations = station_counts.withColumn("rank", F.rank().over(window_spec))
        most_popular_df = ranked_stations.filter(F.col("rank") == 1) \
            .select("start_month", "from_station_id", "from_station_name", F.col("count").alias("trip_count")) \
            .orderBy("start_month")
        return most_popular_df
    except Exception as e:
        logger.error(f"Lỗi khi tính câu 3: {e}", exc_info=True)
        return None

def top_3_stations_last_two_weeks(df: DataFrame) -> Optional[DataFrame]:
    """Câu 4: Top 3 trạm (xuất phát) phổ biến nhất mỗi ngày trong 2 tuần cuối cùng."""
    logger.info("Câu 4: Top 3 trạm phổ biến nhất mỗi ngày trong 2 tuần cuối cùng...")
    try:
        df_filtered = df.filter(F.col("start_time").isNotNull() & F.col("from_station_id").isNotNull() & F.col("from_station_name").isNotNull())
        if df_filtered.rdd.isEmpty():
             logger.warning("Câu 4: Không có dữ liệu hợp lệ ban đầu để tính.")
             return None

        max_date_result = df_filtered.agg(F.max("start_time")).first()
        if not max_date_result or max_date_result[0] is None:
            logger.warning("Câu 4: Không tìm thấy ngày cuối cùng trong dữ liệu hợp lệ.")
            return None
        max_date = max_date_result[0]
        # Tính toán ngày bắt đầu của khoảng thời gian 2 tuần (14 ngày bao gồm ngày cuối)
        # Spark/Python timedelta không trực tiếp trừ ngày như vậy, cần lấy date trước
        max_pyspark_date = F.to_date(F.lit(max_date.strftime('%Y-%m-%d'))) # Lấy ngày từ timestamp
        # Tính toán trong Python vì thao tác với ngày đơn giản hơn
        two_weeks_ago_date = max_date.date() - timedelta(days=13) # 14 ngày tính cả max_date

        logger.info(f"Câu 4: Ngày cuối cùng: {max_date.date()}, Ngày bắt đầu 2 tuần cuối: {two_weeks_ago_date}")

        df_last_two_weeks = df_filtered.filter(F.to_date(F.col("start_time")) >= two_weeks_ago_date) \
                              .withColumn("start_date", F.to_date(F.col("start_time")))

        if df_last_two_weeks.rdd.isEmpty():
             logger.warning("Câu 4: Không có dữ liệu trong khoảng 2 tuần cuối.")
             return None

        station_counts_daily = df_last_two_weeks.groupBy("start_date", "from_station_id", "from_station_name") \
            .count()

        window_spec = Window.partitionBy("start_date").orderBy(F.col("count").desc())
        ranked_stations_daily = station_counts_daily.withColumn("rank", F.rank().over(window_spec))
        top_3_df = ranked_stations_daily.filter(F.col("rank") <= 3) \
            .select("start_date", "rank", "from_station_id", "from_station_name", F.col("count").alias("trip_count")) \
            .orderBy("start_date", "rank")
        return top_3_df
    except Exception as e:
        logger.error(f"Lỗi khi tính câu 4: {e}", exc_info=True)
        return None

def average_duration_by_gender(df: DataFrame) -> Optional[DataFrame]:
    """Câu 5: So sánh thời gian chuyến đi trung bình giữa Nam và Nữ."""
    logger.info("Câu 5: So sánh thời gian chuyến đi trung bình giữa Nam và Nữ...")
    try:
        # Lọc cả tripduration và gender hợp lệ
        df_filtered = df.filter(
            F.col("gender").isin(["Male", "Female"]) & F.col("tripduration").isNotNull()
        )
        if df_filtered.rdd.isEmpty():
             logger.warning("Câu 5: Không có dữ liệu hợp lệ (gender='Male'/'Female', tripduration != NULL) để tính.")
             return None

        gender_duration_df = df_filtered.groupBy("gender") \
            .agg(F.avg("tripduration").alias("average_duration_seconds"))
        return gender_duration_df
    except Exception as e:
        logger.error(f"Lỗi khi tính câu 5: {e}", exc_info=True)
        return None

def top_10_ages_longest_shortest_trips(df: DataFrame) -> Tuple[Optional[DataFrame], Optional[DataFrame]]:
    """Câu 6: Top 10 độ tuổi có chuyến đi dài nhất và ngắn nhất."""
    logger.info("Câu 6: Top 10 độ tuổi có chuyến đi dài nhất và ngắn nhất...")
    try:
        # Lọc dữ liệu cần thiết trước
        df_filtered = df.filter(
            F.col("birthyear").isNotNull() &
            F.col("start_time").isNotNull() &
            F.col("tripduration").isNotNull()
        )
        if df_filtered.rdd.isEmpty():
             logger.warning("Câu 6: Không có dữ liệu hợp lệ (birthyear, start_time, tripduration) để tính tuổi.")
             return None, None

        # Xác định năm hiện tại từ dữ liệu
        current_year_result = df_filtered.agg(F.max(F.year("start_time"))).first()
        if not current_year_result or current_year_result[0] is None:
            logger.warning("Câu 6: Không xác định được năm hiện tại từ dữ liệu để tính tuổi.")
            return None, None
        current_year = current_year_result[0]
        logger.info(f"Câu 6: Năm hiện tại được xác định để tính tuổi: {current_year}")

        df_with_age = df_filtered.withColumn("age", F.lit(current_year) - F.col("birthyear"))

        # Lọc độ tuổi hợp lý (ví dụ: 10 đến 100)
        df_valid_age = df_with_age.filter((F.col("age") >= 10) & (F.col("age") <= 100))
        logger.info(f"Câu 6: Số bản ghi sau khi lọc tuổi hợp lệ: {df_valid_age.count()}") # Có thể chậm nếu dữ liệu lớn

        if df_valid_age.rdd.isEmpty():
             logger.warning("Câu 6: Không còn dữ liệu sau khi lọc tuổi hợp lệ.")
             return None, None

        avg_duration_by_age = df_valid_age.groupBy("age") \
            .agg(F.avg("tripduration").alias("average_duration")) \
            .persist() # Persist kết quả trung gian vì dùng 2 lần

        longest_trips_df = avg_duration_by_age.orderBy(F.col("average_duration").desc()).limit(10)
        shortest_trips_df = avg_duration_by_age.orderBy(F.col("average_duration").asc()).limit(10)

        # Trigger actions để xem kết quả và kiểm tra
        logger.info("Câu 6: Top 10 tuổi có chuyến đi dài nhất (trung bình):")
        longest_trips_df.show(truncate=False)
        logger.info("Câu 6: Top 10 tuổi có chuyến đi ngắn nhất (trung bình):")
        shortest_trips_df.show(truncate=False)

        avg_duration_by_age.unpersist() # Unpersist sau khi dùng xong

        return longest_trips_df, shortest_trips_df
    except Exception as e:
        logger.error(f"Lỗi khi tính câu 6: {e}", exc_info=True)
        if 'avg_duration_by_age' in locals() and avg_duration_by_age.is_cached:
             avg_duration_by_age.unpersist()
        return None, None

# --- Hàm Lưu Báo cáo (Giữ nguyên) ---
def save_report(df: Optional[DataFrame], report_name: str):
    """Lưu Spark DataFrame vào file CSV trong thư mục reports."""
    if df is None:
        logger.warning(f"Không có dữ liệu (DataFrame là None) để lưu cho báo cáo: {report_name}")
        return

    # Kiểm tra xem DataFrame có thực sự chứa dữ liệu không bằng cách thử lấy 1 dòng
    # Dùng head(1) thay vì count() để tránh duyệt toàn bộ dữ liệu lớn chỉ để kiểm tra rỗng
    if not df.head(1):
         logger.warning(f"DataFrame cho báo cáo '{report_name}' rỗng, không ghi file.")
         return

    output_path = REPORTS_DIR / f"{report_name}"
    logger.info(f"Đang lưu báo cáo '{report_name}' vào thư mục: {output_path}")
    try:
        # Coalesce(1) để gộp thành 1 file CSV duy nhất
        df.coalesce(1).write.csv(str(output_path), header=True, mode="overwrite")
        logger.info(f"Lưu báo cáo '{report_name}' thành công vào thư mục {output_path}.")
    except Exception as e:
        logger.error(f"Lỗi khi lưu báo cáo '{report_name}': {e}", exc_info=True)

# --- Luồng Thực thi Chính ---
if __name__ == "__main__":
    logger.info("--- Bắt đầu Exercise 6: PySpark Aggregation ---")

    spark = None
    trips_df_cached = None # Để theo dõi DataFrame đã cache/persist
    try:
        spark = create_spark_session()
        trips_df = read_data(spark, INPUT_FILES_PATTERN, schema)

        if trips_df:
            # Cache DataFrame gốc sau khi đọc và kiểm tra schema thành công
            # Lưu ý: read_data đã persist() df trước khi return, nên có thể dùng trực tiếp
            # Hoặc cache lại ở đây nếu muốn quản lý tập trung
            trips_df_cached = trips_df.cache()
            logger.info("DataFrame chính đã được cache.")

            # Tạo thư mục reports nếu chưa có
            REPORTS_DIR.mkdir(parents=True, exist_ok=True)

            # Thực hiện các phép tính và lưu báo cáo
            report1 = calculate_average_duration_per_day(trips_df_cached)
            save_report(report1, "average_duration_per_day")

            report2 = count_trips_per_day(trips_df_cached)
            save_report(report2, "trips_per_day")

            report3 = most_popular_start_station_per_month(trips_df_cached)
            save_report(report3, "most_popular_start_station_per_month")

            report4 = top_3_stations_last_two_weeks(trips_df_cached)
            save_report(report4, "top_3_stations_last_two_weeks")

            report5 = average_duration_by_gender(trips_df_cached)
            save_report(report5, "average_duration_by_gender")

            report6_longest, report6_shortest = top_10_ages_longest_shortest_trips(trips_df_cached)
            save_report(report6_longest, "top_10_ages_longest_trips")
            save_report(report6_shortest, "top_10_ages_shortest_trips")

        else:
            logger.error("Không thể đọc hoặc xử lý dữ liệu đầu vào. Dừng xử lý.")
            exit(1) # Thoát với mã lỗi

    except Exception as e:
         logger.error(f"Lỗi không mong muốn trong quá trình xử lý chính: {e}", exc_info=True)
         exit(1) # Thoát với mã lỗi
    finally:
        if trips_df_cached:
            logger.info("Đang unpersist DataFrame chính...")
            trips_df_cached.unpersist()
            logger.info("DataFrame chính đã được unpersist.")
        if spark:
            logger.info("Đang dừng SparkSession...")
            spark.stop()
            logger.info("SparkSession đã dừng.")

    logger.info("--- Kết thúc Exercise 6 ---")
