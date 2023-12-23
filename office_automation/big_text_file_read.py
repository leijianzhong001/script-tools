# -*- coding:utf-8 -*-

SRC_FILE_PATH = "D:\\file\\202312\\sentinel日志\\sentinel-67.log"
DST_FILE_PATH = "D:\\file\\202312\\sentinel日志\\sentinel-dst-67.log"


def read_in_chunks(file_path, chunk_size=1024 * 1024 * 10):
    """
    Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1M
    You can set your own chunk size
    """
    file_object = open(file_path)
    while True:
        chunk_data = file_object.read(chunk_size)
        if not chunk_data:
            break
        yield chunk_data


def process(file_chunk):
    """
    处理文件块
    :param file_chunk:
    :return:
    """
    lines = file_chunk.splitlines()
    for line in lines:
        if line.find('[32746] 22 Dec') == -1:
            continue
        with open(DST_FILE_PATH, 'a', encoding='utf-8') as file_obj_w:
            file_obj_w.write(line)
            file_obj_w.write('\n')
            print(line)


if __name__ == "__main__":
    # 先清空文件
    with open(DST_FILE_PATH, 'w', encoding='utf-8') as file_obj:
        pass
    for chunk in read_in_chunks(SRC_FILE_PATH):
        process(chunk)
