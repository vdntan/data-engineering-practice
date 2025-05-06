import logging
import zipfile
import io
import csv
from pathlib import Path
from typing import Optional, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, LongType, IntegerType, DateType
)
from pyspark.sql.window import Window

# --- Cấu hình Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Hằng số và Đường dẫn ---
# <<< SỬA LẠI ĐỂ TRỎ ĐÚNG FILE ZIP >>>
INPUT_ZIP_FILE = Path("data") / "hard-drive-2022-01-01-failures.csv.zip"
# Thư mục tạm không còn cần thiết nếu đọc trực tiếp
# TEMP_EXTRACT_DIR = Path("/tmp/extracted_csv_ex7")

# --- Khởi tạo SparkSession ---
def create_spark_session(app_name="Exercise7"):
    """Khởi tạo và trả về một SparkSession."""
    logger.info(f"Đang khởi tạo SparkSession: {app_name}")
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "4") \
            .getOrCreate()
        # Đặt cấu hình để xử lý timestamp/date đúng định dạng khi cast từ string
        spark.conf.set("spark.sql.datetime.java8API.enabled", "true")
        logger.info("SparkSession đã được tạo thành công.")
        return spark
    except Exception as e:
        logger.error(f"Lỗi khi tạo SparkSession: {e}", exc_info=True)
        raise

# --- Hàm đọc dữ liệu CSV từ bên trong ZIP ---
def read_zipped_csv(spark: SparkSession, zip_file_path: Path) -> Optional[DataFrame]:
    """
    Đọc file CSV đầu tiên tìm thấy bên trong một file ZIP và trả về Spark DataFrame.
    DataFrame trả về sẽ có tất cả các cột dưới dạng StringType ban đầu.
    """
    logger.info(f"Đang đọc dữ liệu từ file ZIP: {zip_file_path}")
    if not zip_file_path.exists():
        logger.error(f"File ZIP không tồn tại: {zip_file_path}")
        return None

    try:
        header = None
        data_lines = []

        with zipfile.ZipFile(zip_file_path, 'r') as zf:
            csv_files_in_zip = [name for name in zf.namelist()
                                if name.lower().endswith('.csv') and "__MACOSX" not in name]

            if not csv_files_in_zip:
                logger.error(f"Không tìm thấy file CSV nào hợp lệ trong: {zip_file_path}")
                return None

            if len(csv_files_in_zip) > 1:
                 logger.warning(f"Tìm thấy nhiều file CSV trong zip: {csv_files_in_zip}. "
                                f"Chỉ sử dụng file đầu tiên: {csv_files_in_zip[0]}")

            csv_file_name = csv_files_in_zip[0]
            logger.info(f"Đang đọc file CSV '{csv_file_name}' từ bên trong '{zip_file_path}'")

            with zf.open(csv_file_name, 'r') as csv_file:
                text_stream = io.TextIOWrapper(csv_file, encoding='utf-8', errors='ignore')
                # Đọc header
                header_line = next(text_stream).strip()
                header = [col.strip() for col in header_line.split(',')] # Đọc header đơn giản bằng split
                logger.info(f"Đã đọc header với {len(header)} cột.")
                # Đọc các dòng dữ liệu còn lại
                data_lines = [line.strip() for line in text_stream if line.strip()]

        if not header or not data_lines:
            logger.error(f"Header hoặc dữ liệu rỗng trong file CSV bên trong {zip_file_path}")
            return None

        # Tạo RDD từ các dòng dữ liệu
        rdd = spark.sparkContext.parallelize(data_lines)

        # Parse RDD sử dụng thư viện csv để xử lý dấu phẩy trong dữ liệu
        expected_columns = len(header)
        def parse_csv_line(line: str):
            try:
                reader = csv.reader([line])
                parsed_list = next(reader)
                # Trả về list nếu số cột khớp, ngược lại trả về None
                return parsed_list if len(parsed_list) == expected_columns else None
            except Exception:
                return None

        parsed_rdd = rdd.map(parse_csv_line).filter(lambda x: x is not None)

        if parsed_rdd.isEmpty():
            logger.error("Không có dòng dữ liệu nào được parse thành công.")
            return None

        # Tạo DataFrame với header đã đọc, tất cả cột là StringType
        df = parsed_rdd.toDF(header)
        record_count = df.count() # Trigger action để kiểm tra
        logger.info(f"Đọc và parse dữ liệu thành công. Số lượng bản ghi hợp lệ: {record_count}")
        return df

    except zipfile.BadZipFile:
        logger.error(f"File không hợp lệ hoặc không phải file ZIP: {zip_file_path}")
        return None
    except Exception as e:
        logger.error(f"Lỗi không mong muốn khi đọc file ZIP {zip_file_path}: {e}", exc_info=True)
        return None

# --- Hàm làm sạch và ép kiểu dữ liệu ---
def cast_types(df: DataFrame) -> DataFrame:
    """Ép kiểu các cột quan trọng về đúng định dạng."""
    logger.info("Thực hiện cast kiểu dữ liệu...")
    try:
        # Ép kiểu Date
        df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        logger.info("Đã cast cột 'date' sang DateType.")

        # Ép kiểu số nguyên/dài
        long_cols = ["capacity_bytes"] + [col for col in df.columns if col.startswith("smart_") and col.endswith("_raw")]
        for col_name in long_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(LongType()))
                # logger.debug(f"Đã cast cột '{col_name}' sang LongType.") # Ghi log chi tiết nếu cần
        logger.info(f"Đã cast các cột raw sang LongType: {long_cols}")

        if "failure" in df.columns:
             df = df.withColumn("failure", F.col("failure").cast(IntegerType()))
             logger.info("Đã cast cột 'failure' sang IntegerType.")

        # Các cột _normalized thường là string hoặc có thể null, giữ nguyên trừ khi có yêu cầu khác
        logger.info("Cast kiểu dữ liệu hoàn tất.")
        return df
    except Exception as e:
        logger.error(f"Lỗi trong quá trình cast kiểu dữ liệu: {e}", exc_info=True)
        # Trả về DataFrame gốc nếu có lỗi để tránh lỗi tiếp theo
        return df


# --- Các Hàm thực hiện yêu cầu bài tập ---

def add_source_file(df: DataFrame, filename: str) -> DataFrame:
    """Câu 1: Thêm cột source_file."""
    logger.info("Câu 1: Thêm cột source_file...")
    return df.withColumn("source_file", F.lit(filename))

def add_file_date(df: DataFrame) -> DataFrame:
    """Câu 2: Thêm cột file_date từ cột source_file."""
    logger.info("Câu 2: Thêm cột file_date...")
    # Trích xuất ngày dạng YYYY-MM-DD từ tên file
    # Pattern: Tìm chuỗi có dạng 4 số - 2 số - 2 số
    date_pattern = r"(\d{4}-\d{2}-\d{2})"
    # regexp_extract trả về chuỗi khớp với group 1, hoặc chuỗi rỗng nếu không khớp
    df = df.withColumn("file_date_str", F.regexp_extract(F.col("source_file"), date_pattern, 1))
    # Chuyển chuỗi ngày sang kiểu Date, nếu chuỗi rỗng hoặc sai định dạng sẽ thành null
    return df.withColumn("file_date", F.to_date(F.col("file_date_str"), "yyyy-MM-dd"))

def add_brand(df: DataFrame) -> DataFrame:
    """Câu 3: Thêm cột brand dựa trên cột model."""
    logger.info("Câu 3: Thêm cột brand...")
    # Tách cột model bằng khoảng trắng đầu tiên
    split_col = F.split(F.col("model"), " ", 2) # Giới hạn tách thành 2 phần
    # Lấy phần tử đầu tiên sau khi tách
    first_part = F.element_at(split_col, 1)
    # Kiểm tra xem cột model có chứa khoảng trắng không
    # Nếu có, lấy first_part, ngược lại là 'unknown'
    return df.withColumn(
        "brand",
        F.when(F.col("model").contains(" "), first_part)
         .otherwise(F.lit("unknown"))
    )

def add_storage_ranking(df: DataFrame) -> DataFrame:
    """Câu 4: Thêm cột storage_ranking dựa trên capacity_bytes."""
    logger.info("Câu 4: Thêm cột storage_ranking...")
    # Tạo DataFrame phụ chứa các giá trị capacity_bytes duy nhất và xếp hạng chúng
    # Điều này phù hợp với mô tả "relates capacity_bytes to the model column"
    # và tạo "buckets"/"rankings" cho capacity.
    logger.info("Tạo bảng xếp hạng dung lượng duy nhất...")
    window_spec_rank = Window.orderBy(F.col("capacity_bytes").desc())
    capacity_ranks_df = df.select("capacity_bytes") \
                          .distinct() \
                          .filter(F.col("capacity_bytes").isNotNull()) \
                          .withColumn("storage_ranking", F.dense_rank().over(window_spec_rank))

    # Log top vài hạng dung lượng để kiểm tra
    logger.info("Top dung lượng và hạng:")
    capacity_ranks_df.show(5, truncate=False)

    # Join bảng xếp hạng này trở lại DataFrame chính
    logger.info("Join bảng xếp hạng dung lượng vào DataFrame chính...")
    # Sử dụng left join để giữ tất cả các dòng gốc, ngay cả khi capacity_bytes là null
    df_joined = df.join(capacity_ranks_df, ["capacity_bytes"], "left")
    return df_joined


def add_primary_key(df: DataFrame, key_columns: List[str]) -> DataFrame:
    """Câu 5: Thêm cột primary_key bằng hash các cột định danh."""
    logger.info(f"Câu 5: Thêm cột primary_key từ các cột: {key_columns}")
    # Đảm bảo các cột key tồn tại
    missing_cols = [col for col in key_columns if col not in df.columns]
    if missing_cols:
        logger.error(f"Các cột để tạo khóa chính bị thiếu: {missing_cols}")
        # Trả về df gốc để tránh lỗi hoặc thêm cột null
        return df.withColumn("primary_key", F.lit(None).cast(StringType()))

    # Tạo một cột tạm thời bằng cách nối các cột key (xử lý NULL an toàn)
    # concat_ws bỏ qua các giá trị NULL
    df = df.withColumn("_temp_pk_concat", F.concat_ws("||", *key_columns))

    # Tạo cột primary_key bằng cách hash cột tạm thời (SHA2-256)
    df = df.withColumn("primary_key", F.sha2(F.col("_temp_pk_concat"), 256))

    # Xóa cột tạm thời
    return df.drop("_temp_pk_concat")

# --- Luồng Thực thi Chính ---
if __name__ == "__main__":
    logger.info("--- Bắt đầu Exercise 7: PySpark Functions (Đọc trực tiếp từ ZIP) ---")

    spark = None
    try:
        # Xác định tên file zip đầu vào
        input_zip_filename = INPUT_ZIP_FILE.name

        spark = create_spark_session()

        # 1. Đọc dữ liệu trực tiếp từ ZIP
        raw_df = read_zipped_csv(spark, INPUT_ZIP_FILE)

        if raw_df is None:
            logger.error("Không thể đọc dữ liệu từ file ZIP. Dừng xử lý.")
            exit(1)

        # Cache DataFrame thô sau khi đọc thành công
        raw_df.cache()
        logger.info("DataFrame thô đã được cache.")

        # 2. Ép kiểu dữ liệu
        typed_df = cast_types(raw_df)

        # 3. Thực hiện các yêu cầu thêm cột
        df_q1 = add_source_file(typed_df, input_zip_filename)
        df_q2 = add_file_date(df_q1)
        df_q3 = add_brand(df_q2)
        df_q4 = add_storage_ranking(df_q3)

        # Xác định các cột để tạo khóa chính
        # Dựa vào cấu trúc file, date, serial_number, model có vẻ là bộ khóa hợp lý
        primary_key_cols = ["date", "serial_number", "model"]
        final_df = add_primary_key(df_q4, primary_key_cols)

        # 4. Hiển thị kết quả cuối cùng
        logger.info("--- Schema cuối cùng ---")
        final_df.printSchema()

        logger.info("--- 10 dòng dữ liệu cuối cùng (có thể bị cắt ngắn) ---")
        final_df.show(10, truncate=True) # truncate=True để log gọn hơn

        # Unpersist DataFrame thô
        raw_df.unpersist()
        logger.info("Đã unpersist DataFrame thô.")

    except Exception as e:
         logger.error(f"Lỗi không mong muốn trong quá trình xử lý chính: {e}", exc_info=True)
         exit(1)
    finally:
        if spark:
            logger.info("Đang dừng SparkSession...")
            spark.stop()
            logger.info("SparkSession đã dừng.")

    logger.info("--- Kết thúc Exercise 7 ---")
