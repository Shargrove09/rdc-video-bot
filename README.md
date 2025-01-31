A service account is a special type of Google account intended to represent a non-human user that needs to authenticate and be authorized to access data in Google APIs [sic].

Since it’s a separate account, by default it does not have access to any spreadsheet until you share it with this account. Just like any other Google account.

Here’s how to get one:

Enable API Access for a Project if you haven’t done it yet.

Go to “APIs & Services > Credentials” and choose “Create credentials > Service account key”.

Fill out the form

Click “Create” and “Done”.

Press “Manage service accounts” above Service Accounts.

Press on ⋮ near recently created service account and select “Manage keys” and then click on “ADD KEY > Create new key”.

Select JSON key type and press “Create”.

You will automatically download a JSON file with credentials. It may look like this:

{
"type": "service_account",
"project_id": "api-project-XXX",
"private_key_id": "2cd … ba4",
"private_key": "-----BEGIN PRIVATE KEY-----\nNrDyLw … jINQh/9\n-----END PRIVATE KEY-----\n",
"client_email": "473000000000-yoursisdifferent@developer.gserviceaccount.com",
"client_id": "473 … hd.apps.googleusercontent.com",
...
}
Remember the path to the downloaded credentials file. Also, in the next step you’ll need the value of client_email from this file.

Very important! Go to your spreadsheet and share it with a client_email from the step above. Just like you do with any other Google account. If you don’t do this, you’ll get a gspread.exceptions.SpreadsheetNotFound exception when trying to access this spreadsheet from your application or a script.

Move the downloaded file to ~/.config/gspread/service_account.json. Windows users should put this file to %APPDATA%\gspread\service_account.json.
