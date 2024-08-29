import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time

def authenticate_google_sheets(credentials_file):
    """Xác thực và kết nối với Google Sheets."""
    creds = Credentials.from_service_account_file(credentials_file, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds)
    service = build('sheets', 'v4', credentials=creds)
    return gc, service

def get_data_from_sheet(gc, sheet_key, worksheet_index):
    """Lấy dữ liệu từ worksheet của Google Sheets."""
    sh = gc.open_by_key(sheet_key)
    worksheet = sh.get_worksheet(worksheet_index)
    return worksheet.get_all_records()  # Không bỏ qua dòng tiêu đề

def filter_names(data, owner_name,row_name):
    """Lọc các tên dựa trên tên chủ sở hữu."""
    filtered_names = [row[row_name] for row in data if row['Chủ sở hữu'].strip() == owner_name]
    return [name for name in filtered_names if name.strip()]

def create_dataframe(names1, names2,row_name):
    """Tạo DataFrame từ hai danh sách tên."""
    combined_names = names1 + names2
    return pd.DataFrame(combined_names, columns=[row_name])

def create_or_open_sheet(gc, sheet_key, sheet_title):
    """Tạo sheet mới hoặc mở sheet hiện có."""
    sh = gc.open_by_key(sheet_key)
    existing_sheets = [sheet.title for sheet in sh.worksheets()]
    if sheet_title in existing_sheets:
        return sh.worksheet(sheet_title)
    return sh.add_worksheet(title=sheet_title, rows="100", cols="20")

def update_sheet_with_data(sheet, names, values, update_range,row_name):
    """Cập nhật dữ liệu vào sheet."""
    values_to_update = [[row_name, 'Số lần xuất hiện']] + list(zip(names, values))
    sheet.update(range_name=update_range, values=values_to_update)
def clear_data_in_range(sheet, start_row, end_row, start_col, end_col):
    """Xóa dữ liệu trong phạm vi chỉ định mà không xóa các hàng hoặc cột."""
    range_to_clear = f'{chr(start_col + 65)}{start_row}:{chr(end_col + 65)}{end_row}'
    sheet.batch_clear([range_to_clear])
    print(f"Đã xóa dữ liệu trong phạm vi: {range_to_clear}")
def save_chart_id_to_sheet(sheet, chart_id,range):
    sheet.update(range_name=range, values=[[chart_id]])

def load_chart_id_from_sheet(sheet,range):
    try:
        return sheet.acell(range).value
    except Exception as e:
        print(f"Đã xảy ra lỗi khi lấy ID biểu đồ từ sheet: {e}")
        return None

def create_chart(service, spreadsheet_id, sheet_id, values, row, column,title,start_column,end_column):
    try:
        requests = [{
            "addChart": {
                "chart": {
                    "spec": {
                        "title": title,
                        "basicChart": {
                            "chartType": "COLUMN",
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": [
                                {"position": "BOTTOM_AXIS", "title": ""},
                                {"position": "LEFT_AXIS", "title": ""}
                            ],
                            "domains": [
                                {"domain": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": len(values) + 1, "startColumnIndex": start_column, "endColumnIndex": start_column+1}]}}}
                            ],
                            "series": [
                                {"series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": len(values) + 1, "startColumnIndex": start_column+1, "endColumnIndex": end_column+1}]}}}
                            ]
                        }
                    },
                    "position": {"overlayPosition": {"anchorCell": {"sheetId": sheet_id, "rowIndex": row, "columnIndex": column}}}
                }
            }
        }]
        batch_update_request = {"requests": requests}
        response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_request).execute()
        chart_id = response['replies'][0]['addChart']['chart']['chartId']
        print(f"Biểu đồ đã được tạo với ID: {chart_id}")
        return chart_id
    except HttpError as err:
        print(f"Đã xảy ra lỗi khi tạo biểu đồ: {err}")
        return None

def update_chart(service, spreadsheet_id, sheet_id, chart_id, values,title,start_column,end_column):
    try:
        requests = [{
            "updateChartSpec": {
                "chartId": chart_id,
                "spec": {
                    "title": title,
                    "basicChart": {
                        "chartType": "COLUMN",
                        "legendPosition": "BOTTOM_LEGEND",
                        "axis": [
                            {"position": "BOTTOM_AXIS", "title": ""},
                            {"position": "LEFT_AXIS", "title": ""}
                        ],
                        "domains": [
                            {"domain": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": len(values) + 1, "startColumnIndex": start_column, "endColumnIndex": start_column+1}]}}}
                        ],
                        "series": [
                            {"series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": len(values) + 1, "startColumnIndex": start_column+1, "endColumnIndex": end_column+1}]}}}
                        ]
                    }
                }
            }
        }]
        batch_update_request = {"requests": requests}
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_request).execute()
        print(f"Biểu đồ với ID {chart_id} đã được cập nhật.")
    except HttpError as err:
        print(f"Đã xảy ra lỗi khi cập nhật biểu đồ: {err}")

def tb1(name,table_name):
    update_range = 'A1:B'
    credentials_file = 'a.json'
    sheet_key = '1phtfYoUPC3Crjf_DrThb2p55M5d_wMUnOpR2_EyirVU'
    worksheet_index1 = 1
    worksheet_index2 = 0
    new_sheet_title = table_name
    gc, service = authenticate_google_sheets(credentials_file)
    data = get_data_from_sheet(gc, sheet_key, worksheet_index2)
    data1 = get_data_from_sheet(gc, sheet_key, worksheet_index1)
    filtered_names = filter_names(data, name,row_name='Mức độ ưu tiên')
    filtered_names1 = filter_names(data1,name,row_name='Mức độ ưu tiên')
    if not filtered_names1 and not filtered_names:
        print("Không có dữ liệu để tạo biểu đồ.")
        return
    df = create_dataframe(filtered_names, filtered_names1,row_name='Mức độ ưu tiên')
    new_sheet = create_or_open_sheet(gc, sheet_key, new_sheet_title)
    counts = df['Mức độ ưu tiên'].value_counts()
    update_sheet_with_data(new_sheet, counts.index.tolist(), counts.values.tolist(), update_range,row_name='Mức độ ưu tiên') 
    clear_data_in_range(new_sheet, start_row=len(counts) + 2, end_row=new_sheet.row_count, start_col=0, end_col=1)
    spreadsheet_id = sheet_key
    chart_id = load_chart_id_from_sheet(new_sheet,range='C1')
    chart_title='Biểu Đồ Độ Ưu Tiên'
    if chart_id:
        update_chart(service, spreadsheet_id, new_sheet.id, chart_id, counts,chart_title,start_column=0,end_column=1)

    else:
        chart_id = create_chart(service, spreadsheet_id, new_sheet.id, counts, row=5, column=0,title=chart_title,start_column=0,end_column=1)
        save_chart_id_to_sheet(new_sheet, chart_id,range='C1')
def tb2(name,table_name):
    update_range = 'F1:G'
    credentials_file = 'app.json'
    sheet_key = '1phtfYoUPC3Crjf_DrThb2p55M5d_wMUnOpR2_EyirVU'
    worksheet_index1 = 1
    worksheet_index2 = 0
    new_sheet_title = table_name
    gc, service = authenticate_google_sheets(credentials_file)
    data = get_data_from_sheet(gc, sheet_key, worksheet_index2)
    data1 = get_data_from_sheet(gc, sheet_key, worksheet_index1)
    filtered_names = filter_names(data, name,row_name='Trạng thái')
    filtered_names1 = filter_names(data1,name,row_name='Trạng thái')
    if not filtered_names1 and not filtered_names:
        print("Không có dữ liệu để tạo biểu đồ.")
        return
    df = create_dataframe(filtered_names, filtered_names1,row_name='Trạng thái')
    new_sheet = create_or_open_sheet(gc, sheet_key, new_sheet_title)
    counts = df['Trạng thái'].value_counts()
    update_sheet_with_data(new_sheet, counts.index.tolist(), counts.values.tolist(), update_range,row_name='Trang thái') 
    clear_data_in_range(new_sheet, start_row=len(counts) + 2, end_row=new_sheet.row_count, start_col=5, end_col=6)
    spreadsheet_id = sheet_key
    chart_id = load_chart_id_from_sheet(new_sheet,range='H1')
    chart_title='Biểu Đồ Trạng Thái Công Việc'
    if chart_id:
        update_chart(service, spreadsheet_id, new_sheet.id, chart_id, counts,title=chart_title,start_column=5,end_column=6)
    else:
        chart_id = create_chart(service, spreadsheet_id, new_sheet.id, counts, row=5, column=5,title=chart_title,start_column=5,end_column=6)
        save_chart_id_to_sheet(new_sheet, chart_id,range='H1')  
def tb3(name,table_name):
    update_range = 'K1:L'
    credentials_file = 'ad.json'
    sheet_key = '1phtfYoUPC3Crjf_DrThb2p55M5d_wMUnOpR2_EyirVU'
    worksheet_index1 = 1
    worksheet_index2 = 0
    new_sheet_title = table_name
    gc, service = authenticate_google_sheets(credentials_file)
    data = get_data_from_sheet(gc, sheet_key, worksheet_index2)
    data1 = get_data_from_sheet(gc, sheet_key, worksheet_index1)
    filtered_names = filter_names(data, name,row_name='Category')
    filtered_names1 = filter_names(data1,name,row_name='Category')
    if not filtered_names1 and not filtered_names:
        print("Không có dữ liệu để tạo biểu đồ.")
        return
    df = create_dataframe(filtered_names, filtered_names1,row_name='Category')
    new_sheet = create_or_open_sheet(gc, sheet_key, new_sheet_title)
    counts = df['Category'].value_counts()
    update_sheet_with_data(new_sheet, counts.index.tolist(), counts.values.tolist(), update_range,row_name='Category') 
    clear_data_in_range(new_sheet, start_row=len(counts) + 2, end_row=new_sheet.row_count, start_col=10, end_col=11)
    spreadsheet_id = sheet_key
    chart_id = load_chart_id_from_sheet(new_sheet,range='M1')
    chart_title='Biểu Đồ Category'
    if chart_id:
        update_chart(service, spreadsheet_id, new_sheet.id, chart_id, counts,chart_title,start_column=10,end_column=11)
    else:
        chart_id = create_chart(service, spreadsheet_id, new_sheet.id, counts, row=1, column=12,title=chart_title,start_column=10,end_column=11)
        save_chart_id_to_sheet(new_sheet, chart_id,range='M1')        
def CV(name,table_name):
    tb1(name,table_name)
    tb2(name,table_name)
    tb3(name,table_name)
if __name__ == '__main__':
   CV(name='Đỗ Phương Nam',table_name='Biểu Đồ CV ĐPN')
   CV(name='Nguyễn Đình Thắng',table_name='Biểu Đồ CV NĐT')
   time.sleep(60)
   CV(name='Phạm Thị Hà',table_name='Biểu Đồ CV PTH')
   CV(name='Nguyễn Văn Khánh',table_name='Biểu Đồ CV NVK')
