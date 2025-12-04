
GET_ALL_FACILITIES_URL = "https://spothero.com/api/v1/facilities/operator-facility-search/"
REQUIRED_COLUMNS = ['id', 'spot', 'type', 'reservation status', 'starts', 'ends',
       'license plate', 'make', 'model', 'sale price', 'remit amount',
       'date purchased', 'timezone', 'company', 'spothero city', 'vendor id',
       'purchase_platform', 'renter_full_name', 'renter_phone_number',
       'renter_email']

RENT_CSV_PATH = 'stall_rents.csv'
STALL_ADDITIONS_CSV_PATH = 'stall_additions.csv'
MODEL_PATH = 'python_pricing_model_11_months.xlsx'

FLEX_RATES_REQUIRED_KEYS = ['duration_in_min',
 'interpolation',
 'rate_bands',
 'rate_segments',
 'rate_triggered_segments']

WEEK_DAYS = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday"]
DURATIONS = [
    "01:00",
    "03:00",
    "06:00",
    "12:00",
    "24:00"
]

FLEX_RATE_SHEET_CELLS = [
    ['D10','E10','F10','G10','H10','I10','J10'],
    ['D11','E11','F11','G11','H11','I11','J11'],
    ['D12','E12','F12','G12','H12','I12','J12'],
    ['D13','E13','F13','G13','H13','I13','J13'],
    ['D14','E14','F14','G14','H14','I14','J14']
]

SLEEP_TIME = 0