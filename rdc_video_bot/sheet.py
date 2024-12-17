import gspread

if __name__ == "__main__": 
    gc = gspread.service_account()

    sh = gc.open("Project RDC Video Tracker")

    print(sh.sheet1.get('A1'))

   