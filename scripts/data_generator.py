import os

def create_dummy_csv(filepath, department):
    """Creates a simple placeholder CSV file."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            if department.lower() == 'sales':
                f.write("Name,Email,LeadScore\n")
                f.write("Alice Test,alice@example.com,85\n")
                f.write("Bob Sample,bob@sample.org,92\n")
            elif department.lower() == 'marketing':
                f.write("FirstName,LastName,Email,CampaignSource\n")
                f.write("Charlie,Testington,charlie@test.co,Website\n")
                f.write("Diana,Sampler,diana@mail.net,Webinar\n")
            else:
                f.write("Column1,Column2\n") # Generic fallback
                f.write("Data1,Data2\n")
        print(f"Successfully created dummy data file: {filepath}")
        return True
    except Exception as e:
        print(f"Error creating dummy data file {filepath}: {e}")
        return False