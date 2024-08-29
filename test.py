import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import re

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

def filter_names(data, row_name):
    """Lọc các tên dựa trên tên chủ sở hữu."""
    filtered_names = [row[row_name] for row in data]
    return [name for name in filtered_names if name.strip()]

def create_dataframe(names1,row_name):
    """Tạo DataFrame từ hai danh sách tên."""
    combined_names = names1 
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

def create_chart(service, spreadsheet_id, sheet_id, values, row, column,title,start_column,end_column,row_index):
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
                                {"domain": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": row_index, "endRowIndex": len(values) + row_index, "startColumnIndex": start_column, "endColumnIndex": start_column+1}]}}}
                            ],
                            "series": [
                                {"series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": row_index, "endRowIndex": len(values) + row_index, "startColumnIndex": start_column+1, "endColumnIndex": end_column+1}]}}}
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

def update_chart(service, spreadsheet_id, sheet_id, chart_id, values,title,start_column,end_column,row_index):
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
                            {"domain": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": row_index, "endRowIndex": len(values) + row_index, "startColumnIndex": start_column, "endColumnIndex": start_column+1}]}}}
                        ],
                        "series": [
                            {"series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": row_index, "endRowIndex": len(values) + row_index, "startColumnIndex": start_column+1, "endColumnIndex": end_column+1}]}}}
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
def tb1(t,worksheet_index1, update_range, range, start_column, end_column, row, column, credentials_file='a.json'):
    sheet_key = '1phtfYoUPC3Crjf_DrThb2p55M5d_wMUnOpR2_EyirVU'
    gc, service = authenticate_google_sheets(credentials_file)
    sh = gc.open_by_key(sheet_key)
    worksheets = sh.worksheets()
    sheet_name = worksheets[worksheet_index1].title
    new_sheet_title = 'Biểu Đồ TC ' +str(t)
    data1 = get_data_from_sheet(gc, sheet_key, worksheet_index1)
    filtered_names1 = filter_names(data1, row_name='Result')
    if not filtered_names1:
        print("Không có dữ liệu để tạo biểu đồ.")
        return
    df = create_dataframe(filtered_names1, row_name='Result')
    new_sheet = create_or_open_sheet(gc, sheet_key, new_sheet_title)
    counts = df['Result'].value_counts()
    update_sheet_with_data(new_sheet, counts.index.tolist(), counts.values.tolist(), update_range, row_name='Result')

    # Xóa dữ liệu cũ để đảm bảo bảng sạch sẽ
    #clear_data_in_range(new_sheet, start_row=len(counts) + 2, end_row=new_sheet.row_count, start_col=0, end_col=1)
    clear_data_in_range(new_sheet, start_row=len(counts) + 2, end_row=20, start_col=start_column, end_col=end_column)
    spreadsheet_id = sheet_key
    chart_id = load_chart_id_from_sheet(new_sheet, range=range)
    chart_title = 'Biểu Đồ Result ' +sheet_name
    if chart_id:
        update_chart(service, spreadsheet_id, new_sheet.id, chart_id, counts, chart_title, start_column=start_column, end_column=end_column,row_index=1)
    else:
        chart_id = create_chart(service, spreadsheet_id, new_sheet.id, counts, row=row, column=column, title=chart_title, start_column=start_column, end_column=end_column,row_index=1)
        save_chart_id_to_sheet(new_sheet, chart_id, range=range)
def tb2(t,worksheet_index1, update_range, range, start_column, end_column, row, column, credentials_file='app.json'):
    match = re.search(r'(\d+):', update_range)
    if match:
        temp=int(match.group(1))
    sheet_key = '1phtfYoUPC3Crjf_DrThb2p55M5d_wMUnOpR2_EyirVU'
    gc, service = authenticate_google_sheets(credentials_file)
    sh = gc.open_by_key(sheet_key)
    worksheets = sh.worksheets()
    sheet_name = worksheets[worksheet_index1].title
    new_sheet_title = 'Biểu Đồ TC ' +str(t)
    data1 = get_data_from_sheet(gc, sheet_key, worksheet_index1)
    filtered_names1 = filter_names(data1, row_name='Test date')
    if not filtered_names1:
        print("Không có dữ liệu để tạo biểu đồ.")
        return
    df = create_dataframe(filtered_names1, row_name='Test date')
    new_sheet = create_or_open_sheet(gc, sheet_key, new_sheet_title)
    counts = df['Test date'].value_counts()
    update_sheet_with_data(new_sheet, counts.index.tolist(), counts.values.tolist(), update_range, row_name='Test date')

    # Xóa dữ liệu cũ để đảm bảo bảng sạch sẽ
    clear_data_in_range(new_sheet, start_row=len(counts) + 1+temp, end_row=new_sheet.row_count, start_col=start_column, end_col=end_column)
    
    spreadsheet_id = sheet_key
    chart_id = load_chart_id_from_sheet(new_sheet, range=range)
    chart_title = 'Biểu Đồ Test date ' +sheet_name
    if chart_id:
        update_chart(service, spreadsheet_id, new_sheet.id, chart_id, counts, chart_title, start_column=start_column, end_column=end_column,row_index=temp)
    else:
        chart_id = create_chart(service, spreadsheet_id, new_sheet.id, counts, row=row, column=column, title=chart_title, start_column=start_column, end_column=end_column,row_index=temp)
        save_chart_id_to_sheet(new_sheet, chart_id, range=range)
def CV(t,worksheet_index1, update_range1,update_range2,range1,range2, start_column, end_column, row1,row2, column): 
    tb1(t,worksheet_index1, update_range=update_range1, range=range1, start_column=start_column, end_column=end_column, row=row1, column=column)
    tb2(t,worksheet_index1, update_range=update_range2, range=range2, start_column=start_column, end_column=end_column, row=row2, column=column)

if __name__ == '__main__':
    i=6
    for t in range(0,6):
       CV(t+1,worksheet_index1=i, update_range1='A1:B',update_range2='A24:B',range1='C1' ,range2='C24', start_column=0, end_column=1, row1=5,row2=32, column=0)
       CV(t+1,worksheet_index1=i+1, update_range1='F1:G',update_range2='F24:G',range1='H1' ,range2='H24', start_column=5, end_column=6, row1=5,row2=32, column=5)
       CV(t+1,worksheet_index1=i+2, update_range1='K1:L',update_range2='K24:L',range1='M1' ,range2='M24', start_column=10, end_column=11, row1=5,row2=32, column=10)
       i=i+3
       time.sleep(30)


