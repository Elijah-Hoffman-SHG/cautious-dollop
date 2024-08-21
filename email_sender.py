import inspect
import smtplib
import ssl
from email.mime.text import MIMEText
from datetime import datetime
from typing import List
from abc import ABC, abstractmethod


class EmailRowBase(ABC):
    def __init__(self, vals, title="", corner_header = ""):
        self.vals = vals
        self.title = title
        self.corner_header =corner_header

    @abstractmethod
    def format_table_row(self):
        raise NotImplementedError

class EmailRowExp:
    def __init__(self, data):
        current_frame = inspect.currentframe()
        caller_frame = inspect.getouterframes(current_frame)[1]
        local_vars = caller_frame.frame.f_locals
    
        for name, value in local_vars.items():
            if value is data:
                print(f"So da name is here {name}")
                print(name.replace('_', ' ').title())
                self.title = name.replace('_', ' ').title()
                self.val = data
    
               
class EmailRowDict(EmailRowBase):

    def format_table_row(self):
         # Extract headers from the first nested dictionary
        data = self.vals
        print(len(data))
        print(type(data))
        headers = list(next(iter(data.values())).keys())
        
        # Start building the HTML table
        html = f"<tr><td>{self.title}</td><td>"
        html += "<table border='1'>"
        
        # Create the table header row
        html += f"<tr><th>{self.corner_header}</th>"
        for header in headers:
            html += f"<th>{header}</th>"
        html += "</tr>"
        
        # Create the table rows
        for key, values in data.items():
            html += f"<tr><td>{key}</td>"
            for header in headers:
                html += f"<td>{values[header]}</td>"
            html += "</tr>"
        
        html += "</table>"
        return html





class EmailRow(EmailRowBase):
    def __init__(self, vals, headers='', corner_header='', title =''):
        super().__init__(vals, title)
        self.headers = headers
        self.corner_header = corner_header
        self.title = title
        if title=='':
            current_frame = inspect.currentframe()
            caller_frame = inspect.getouterframes(current_frame)[1]
            local_vars = caller_frame.frame.f_locals
            for name, value in local_vars.items():
                if value is vals:
                    print(f"So da name is here {name}")
                    print(name.replace('_', ' ').title())
                    self.title = name.replace('_', ' ').title()
        
    def format_table_row(self):
        html = f"<tr><td>{self.title}</td><td>"
        if self.headers!='' and isinstance(self.vals, dict):
            corner_header = ''
            for key, value in self.vals.items():
                if len(self.headers) == len(value)+1:
                    print('hellooooo')
                    print(self.headers)
                    print(f"key: {key}")
                    print(f"vallue: {value}")
                    corner_header=self.headers[0]
                assert len(self.headers) == len(value) or len(self.headers) == len(value)+1, f"Mismatch in length for key '{key}': length of headers: {len(self.headers)} length of row: {len(value)}"
            
            html += "<table border='1'>"
            html += f"<tr><th>{corner_header}</th>"
            for header in self.headers:
                if header != corner_header:
                    html += f"<th>{header}</th>"
            html += "</tr>"
            
            for name, numbers in self.vals.items():
                html += "<tr>"
                html += f"<td>{name}</td>"
                for number in numbers:
                    html += f"<td>{number}</td>"
                html += "</tr>"
            
            html += "</table>"
        else:
            html+=str(self.vals)
        html+="</td></tr>"
        return html


class EmailSection:
    def __init__(self, title, rows:List[EmailRowBase]):
        self.title = title
        self.rows = rows
    def format_table(self):
        html = "<table border='1'>\n"
        html += f" <tr><th colspan='2'>{self.title}</th></tr>\n"

        for row in self.rows:
            html += row.format_table_row()
        html+= "</table>"
        return html



class TableHelp:
    def __init__(self):
        self.headers = ['', 'email', 'total']
        self.vals = {
            'siml4': [2257, 6679],
            'wiml4': [2257, 6679]
        }
    def format_table_with_headers(self, headers, vals):
        """
        Generates an HTML table with headers

        Args:
            headers (list): A list of column headers.
            vals (dict): A dictionary where keys are row names and values are lists of column values.

        Returns:
            str: An HTML string representing the table.

        Raises:
            AssertionError: If the number of headers does not match the number of values in any entry of `vals`.

        Example:
            headers = ['email', 'total']
            vals = {
                'siml4': [2257, 6679],
                'wiml4': [2257, 6679]
            }
        """
        for key, value in vals.items():
            assert len(headers) == len(value), f"Mismatch in length for key '{key}': expected {len(headers)}, got {len(value)}"
        
        html = "<table border='1'>"
        html += "<tr><th></th>"
        for header in headers:
            html += f"<th>{header}</th>"
        html += "</tr>"
        
        for name, numbers in vals.items():
            html += "<tr>"
            html += f"<td>{name}</td>"
            for number in numbers:
                html += f"<td>{number}</td>"
            html += "</tr>"
        
        html += "</table>"
        return html
     

class EmailSender:
    def __init__(self):
        self.port = 25
        self.smtp_server = "outbound.sandhills.com"
        self.sender_email = "your-friend@sandhills.com"
        self.receiver_emails = ['elijah-hoffman@sandhills.com']
        self.context = ssl.create_default_context()
        self.head = """
        <head>
            <style>
                body {
                    font-family: Arial, sans-serif;
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                }
                th, td {
                    border: 1px solid #333;
                    width: 50%;
                    padding: 1rem;
                    text-align: left;
                }
                th {
                    background-color: #333;
                }
                tr:nth-child(even) {
                    background-color: #1e1e1e;
                }
                tr:nth-child(odd) {
                    background-color: #2e2e2e;
                }
            </style>
        </head>
    """
    def create_section_new(self, list_of_vals_for_rows, name_str = '', section_title='error'):
        print("------------------\n\nhello friend---------\n\n")
        print(list_of_vals_for_rows)
        print("------------------\n\nhello friend---------\n\n")
        current_frame = inspect.currentframe()
        caller_frame = inspect.getouterframes(current_frame)[1]
        local_vars = caller_frame.frame.f_locals
        
        # Find the variable name for the data
        if (section_title=='error'):
            variable_name = None
            for name, val in local_vars.items():
                if (name is name_str):
                    print(val)
                if val is list_of_vals_for_rows:
                    print(name)
                    section_title = name.replace('_', ' ').title()
                    break
            
        
        rows =[]
        for item in list_of_vals_for_rows:
            if isinstance(item, tuple):
                print(f"{item} is a tuple")
                if len(item) == 3:
                    value, headers, corner_header = item
                else:
                    value, corner_header = item
                
            else:
                value = item
                headers = None
                corner_header = ''
            
            current_frame = inspect.currentframe()
            caller_frame = inspect.getouterframes(current_frame)[1]
            local_vars = caller_frame.frame.f_locals
            
            variable_name = None
            for name, val in local_vars.items():
                if val is value:
                    variable_name = name
                    break
            
            if variable_name:
                print("WeGotVariableName")
                print(variable_name)
                
                formatted_name = variable_name.replace('_', ' ').title()
                if(isinstance(value, dict)):
                    rows.append(EmailRowDict(value, formatted_name,corner_header))
                else:
                    rows.append(EmailRow(value, headers, title = formatted_name, corner_header=corner_header))
                
        return EmailSection(section_title, rows)
    def create_email(self, big_data:List[EmailSection]):
        print("Big data in create_email")
        print(big_data)
        html = self.head
        html +=f"<body><h1>Recsys Summary Report for {datetime.now().date()}</h1>"
        for data in big_data:
            html+= data.format_table()
            html+= "<br>"
        html+='</body>'
        return html
    
    def create_section(self, title, data_dict):
        html = "<table border='1'>\n"
        html += f" <tr><th colspan='2'>{title}</th></tr>\n"
        html+=self.create_table(data_dict)
        return html
    
    def create_table(self, data_dict):
        html =""
        for name, value in data_dict.items():
            if isinstance(value, dict):
                html += f"  <tr><td>{name}</td><td>\n"
                html += "    <table>\n"
                html += "      <tr>"
                for sub_name in value.keys():
                    html += f"<td>{sub_name}</td>"
                html += "</tr>\n"
                html += "      <tr>"
                for sub_value in value.values():
                    html += f"<td>{sub_value}</td>"
                html += "</tr>\n"
                html += "    </table>\n"
                html += "  </td></tr>\n"
            else:
                # If the value is not a dictionary, create a regular table row
                html += f"  <tr><td>{name}</td><td>{value}</td></tr>\n"
        html += "</table>"
        return html
    
    


    def send_html(self, html):
        message = MIMEText(html, "html")
        today = datetime.now().date()
      
        message["Subject"] = f"Recsys Summary Fron Sebd HTML Report: {today}"
        message["From"] = self.sender_email
        message["To"] = ", ".join(self.receiver_emails)

        with smtplib.SMTP(self.smtp_server, self.port) as server:
            server.starttls(context=self.context)
            for receiver_email in self.receiver_emails:
                server.sendmail(self.sender_email, receiver_email, message.as_string())
                print(f"Email sent to {receiver_email}")

    def send_emails(self, data_dict):
        
        html_content = self.create_email(data_dict)
        message = MIMEText(html_content, "html")
        today = datetime.now().date()
      
        message["Subject"] = f"Recsys Summary Report: {today}"
        message["From"] = self.sender_email
        message["To"] = ", ".join(self.receiver_emails)

        with smtplib.SMTP(self.smtp_server, self.port) as server:
            server.starttls(context=self.context)
            for receiver_email in self.receiver_emails:
                server.sendmail(self.sender_email, receiver_email, message.as_string())
                print(f"Email sent to {receiver_email}")
