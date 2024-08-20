import smtplib
import ssl
from email.mime.text import MIMEText
from datetime import datetime

class TableHelp:
    def __init__(self):
        self.headers = ['', 'email', 'total']
        self.vals = {
            'siml4': [2257, 6679],
            'wiml4': [2257, 6679]
        }
    def format_table(self):
        html = "<table border='1'>"
        html += "<tr>"
        for header in self.headers:
            html += f"<th>{header}</th>"
        html += "</tr>"
        
        for name, numbers in self.vals.items():
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
                    padding: 8px;
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

    def create_email(self, big_data):
        html = self.head
        html +=f"<body><h1>Recsys Summary Report for {datetime.now().date()}</h1>"
        
        for name, value in big_data.items():
            html+=self.create_section(name, value)
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
    
    


    def send_emails(self, data_dict):
        html_content = TableHelp().format_table()
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
