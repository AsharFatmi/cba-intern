import streamlit as st
import requests
import mysql.connector
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
st.markdown("""<style>#MainMenu {visibility: hidden;}</style>""", unsafe_allow_html=True)
st.markdown("""<style>header {visibility: hidden;}""", unsafe_allow_html=True)
# Function to authenticate user
def authenticate_user(username, password):
  try:
      connection = mysql.connector.connect(
          host='cbaworks.org',
          database='vacancy_abatement',
          user=username,
          password=password
      )
      if connection.is_connected():
          return connection
      else:
          return None
  except mysql.connector.Error as err:
      st.error(f"Error: {err}")
      return None

# Function to clean up monetary values and format with two decimal places, without commas
def clean_up_monetary_value(value):
  if pd.isna(value):
      return None
  cleaned_value = str(value).replace('$', '').replace(',', '')
  try:
      return float(cleaned_value)
  except ValueError:
      return None

# Function to clean up square footage and ensure no commas
def clean_up_square_footage(value):
  if pd.notna(value):
      cleaned_value = value.replace(',', '').replace('*', '')
      try:
          return int(cleaned_value)
      except ValueError:
          return None
  return None

# Function to clean property data and apply formatting rules
def clean_property_data(property_data):
  # Clean the PIN field to remove dashes and commas, ensure it's a string of numbers
  if pd.notna(property_data.get('Pin')):
      if isinstance(property_data['Pin'], str):
          property_data['Pin'] = property_data['Pin'].replace('-', '').replace(',', '')  # Remove dashes and commas from PIN
   # Clean up square footage to ensure no commas
  if 'SquareFootage' in property_data:
      property_data['SquareFootage'] = clean_up_square_footage(property_data['SquareFootage'])
   # Clean up and format monetary values without commas and with two decimal places
  for key in ['PreviousBoardCertified', 'AssessorValuation', 'AssessorPostAppealValuation']:
      if key in property_data:
          value = clean_up_monetary_value(property_data[key])
          if value is not None:
              property_data[key] = f"{value:.2f}"  # Format to two decimal places, no commas
  return property_data

# Function to scrape property data with enhanced logic
def scrape_property_data(pin_number):
  base_url = 'https://www.cookcountyassessor.com/pin/'
  url = f'{base_url}{pin_number}'  # Construct the URL
  try:
      response = requests.get(url)
      if response.status_code == 200:
          html_text = response.text
          soup = BeautifulSoup(html_text, 'html.parser')
          # Map labels to corresponding column names
          label_to_column = {
              'Pin': 'Pin',
              'Address': 'StreetNumber',
              'City': 'City',
              'Township': 'Township',
              'Property Classification': 'PropertyClassification',
              'Square Footage (Land)': 'SquareFootage',
              'Neighborhood': 'Neighborhood',
              'Taxcode': 'Taxcode',
              'Next Scheduled Reassessment': 'NextScheduledReassessment',
              'Description': 'Description',
              'Age': 'Age',
              'Building Square Footage': 'BuildingSquareFootage',
              'Assessment Phase': 'AssessmentPhase',
              'Previous Board Certified': 'PreviousBoardCertified',
              'Status': 'Status',
              'Assessor Valuation': 'AssessorValuation',
              'Assessor Post-Appeal Valuation': 'AssessorPostAppealValuation',
              'Appeal Number': 'AppealNumber',
              'Attorney/Tax Representative': 'AttorneyTaxRepresentative',
              'Applicant': 'Applicant',
              'Result': 'Result',
              'Reason': 'Reason',
              'Tax Year': 'TaxYear',
              'Certificate Number': 'CertificateNumber',
              'Property Location': 'PropertyLocation',
              'C of E Description': 'COfEDescription',
              'Comments': 'Comments',
              'Residence Type': 'ResidenceType',
              'Use': 'Use',
              'Apartments': 'Apartments',
              'Exterior Construction': 'ExteriorConstruction',
              'Full Baths': 'FullBaths',
              'Half Baths': 'HalfBaths',
              'Basement1': 'Basement1',
              'Attic': 'Attic',
              'Central Air': 'CentralAir',
              'Number of Fireplaces': 'NumberOfFireplaces',
              'Garage Size/Type2': 'GarageSizeType2'
          }
          property_data = {'Pin': pin_number, 'Link': url}  # Include the URL in the data
          detail_rows = soup.find_all(['div', 'span'], class_=[
              'detail-row', 'detail-row--label', 'col-xs-3 pt-header',
              'col-xs-2', 'detail-row--detail', 'large', 'col-xs-4',
              'col-xs-5', 'small'
          ])
          column_name = None
          for row in detail_rows:
              if 'detail-row--label' in row.get('class', []):
                  label = row.text.strip()
                  if label in label_to_column:
                      column_name = label_to_column[label]
                      property_data[column_name] = None
              elif 'detail-row--detail' in row.get('class', []):
                  value = row.text.strip()
                  if column_name in property_data:
                      property_data[column_name] = value
          # Clean the property data
          property_data = clean_property_data(property_data)
          return property_data
      else:
          st.error(f"Failed to retrieve page for PIN {pin_number}, status code: {response.status_code}")
          return None
  except requests.RequestException as e:
      st.error(f"Request failed for PIN {pin_number}: {e}")
      return None

# Function to authenticate user
def authenticate_user(username, password):
  try:
      connection = mysql.connector.connect(
          host='cbaworks.org',
          database='vacancy_abatement',
          user=username,
          password=password
      )
      if connection.is_connected():
          return connection
      else:
          return None
  except mysql.connector.Error as err:
      st.error(f"Error: {err}")
      return None


# Function to fetch data from an external API
def fetch_api_data(street_number):
  url = "https://data.cityofchicago.org/resource/22u3-xenr.json"
  try:
      response = requests.get(url, params={"$q": street_number})
      response.raise_for_status()
      return response.json()
  except requests.RequestException as e:
      st.error(f"Error fetching API data: {e}")
      return None
  
# Function to insert data into the database
def insert_into_database(street_number, pin, owner, owner_address, city, state, zip_code, square_feet, link, violations_data, property_data):
  connection = authenticate_user(st.session_state.username, st.session_state.password)
  if connection is None:
      st.error("Failed to connect to the database.")
      return
  try:
      cursor = connection.cursor()
      cursor.execute("""
          INSERT INTO properties (StreetNumber, PIN, Owner, OwnerAddress, City, State, ZIP, SquareFeet, Link)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
      """, (street_number, pin, owner, owner_address, city, state, zip_code, square_feet, link))
      property_id = cursor.lastrowid
      # Insert violations data
      for _, violation in violations_data.iterrows():
          cursor.execute("""
              INSERT INTO violations (PropertyID, violation_code, violation_description, violation_status, violation_date)
              VALUES (%s, %s, %s, %s, %s)
          """, (property_id, violation['violation_code'], violation['violation_description'], violation['violation_status'], violation['violation_date']))

      # Insert property data
      cursor.execute("""
          INSERT INTO property_details (PropertyID, DetailKey, DetailValue)
          VALUES (%s, %s, %s)
      """, (property_id, 'AssessorData', str(property_data)))
      connection.commit()
      st.success("Data successfully inserted into the database.")
  except mysql.connector.Error as err:
      st.error(f"Database error: {err}")
      connection.rollback()
  finally:
      cursor.close()
      connection.close()

def remove_building_page():
    st.title("Remove Building Data")

    # PIN input from the user
    pin = st.text_input("Enter the 14-digit Building PIN for removal")

    if pin and st.button("Fetch Building Data"):
        # Fetch data from all tables based on the entered PIN
        properties_data, details_data, violations_data = fetch_building_data(pin)
        
        if properties_data is None:
            st.error(f"No data found for PIN {pin}")
            return
        
        # Display data from `properties`
        st.subheader("Properties Data")
        properties_df = pd.DataFrame([properties_data], index=["Properties"])
        selected_properties = st.data_editor(properties_df, num_rows="dynamic")

        # Display data from `property_details`
        st.subheader("Property Details Data")
        details_df = pd.DataFrame(details_data)
        selected_details = st.data_editor(details_df, num_rows="dynamic")

        # Display data from `violations`
        st.subheader("Violations Data")
        violations_df = pd.DataFrame(violations_data)
        selected_violations = st.data_editor(violations_df, num_rows="dynamic")

        # Button to confirm deletion of selected rows
        if st.button("Confirm Deletion"):
            remove_selected_rows(pin, selected_properties, selected_details, selected_violations)
            st.success(f"Selected rows for PIN {pin} have been deleted.")

def remove_from_database(pin):
    connection = authenticate_user(st.session_state.username, st.session_state.password)
    if connection is None:
        st.error("Failed to connect to the database.")
        return

    try:
        cursor = connection.cursor()

        # Fetch property data
        cursor.execute("SELECT * FROM properties WHERE TRIM(PIN) = %s", (pin.strip(),))
        property_data = cursor.fetchall()
        if not property_data:
            st.warning(f"No property found for PIN {pin}.")
            return
        
        # Display property data to the user with checkboxes for selection
        st.subheader("Property Information")
        property_df = pd.DataFrame(property_data, columns=[desc[0] for desc in cursor.description])
        selected_property_ids = []

        for i, row in property_df.iterrows():
            # Convert the row to a string for display
            row_display = ', '.join([f"{col}: {row[col]}" for col in property_df.columns])
            if st.checkbox(f"Select row: {row_display}", key=f"property_{i}"):
                selected_property_ids.append(row['PropertyID'])

        # Fetch assessor details
        cursor.execute("""
            SELECT * FROM property_details WHERE PropertyID IN (SELECT PropertyID FROM properties WHERE TRIM(PIN) = %s)
        """, (pin.strip(),))
        assessor_data = cursor.fetchall()
        
        st.subheader("Assessor Details")
        selected_assessor_ids = []
        if assessor_data:
            assessor_df = pd.DataFrame(assessor_data, columns=[desc[0] for desc in cursor.description])
            for i, row in assessor_df.iterrows():
                # Convert the row to a string for display
                row_display = ', '.join([f"{col}: {row[col]}" for col in assessor_df.columns])
                if st.checkbox(f"Select row: {row_display}", key=f"assessor_{i}"):
                    selected_assessor_ids.append(row['PropertyID'])
        else:
            st.write("No assessor data found.")

        # Fetch violation details
        cursor.execute("""
            SELECT * FROM violations WHERE PropertyID IN (SELECT PropertyID FROM properties WHERE TRIM(PIN) = %s)
        """, (pin.strip(),))
        violations_data = cursor.fetchall()
        
        st.subheader("Violations Data")
        selected_violation_ids = []
        if violations_data:
            violations_df = pd.DataFrame(violations_data, columns=[desc[0] for desc in cursor.description])
            for i, row in violations_df.iterrows():
                # Convert the row to a string for display
                row_display = ', '.join([f"{col}: {row[col]}" for col in violations_df.columns])
                if st.checkbox(f"Select row: {row_display}", key=f"violation_{i}"):
                    selected_violation_ids.append(row['PropertyID'])
        else:
            st.write("No violation data found.")

        # Once user selects the rows to delete
        if st.button("Delete Selected"):
            if selected_property_ids:
                cursor.execute("""
                    DELETE FROM properties WHERE PropertyID IN (%s)
                """ % ','.join(map(str, selected_property_ids)))

            if selected_assessor_ids:
                cursor.execute("""
                    DELETE FROM property_details WHERE PropertyID IN (%s)
                """ % ','.join(map(str, selected_assessor_ids)))

            if selected_violation_ids:
                cursor.execute("""
                    DELETE FROM violations WHERE PropertyID IN (%s)
                """ % ','.join(map(str, selected_violation_ids)))

            connection.commit()
            st.success("Selected data deleted successfully.")
    
    except mysql.connector.Error as err:
        st.error(f"Database error: {err}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

# Function to fetch data from the three tables based on the entered PIN
def fetch_building_data(pin):
    # Assuming 'authenticate_user' gets the database connection
    connection = authenticate_user(st.session_state.username, st.session_state.password)
    if connection is None:
        st.error("Failed to connect to the database.")
        return None, None, None
    
    try:
        cursor = connection.cursor()

        # Fetch property data
        cursor.execute("SELECT * FROM properties WHERE TRIM(PIN) = %s", (pin.strip(),))
        property_data = cursor.fetchone()  # Fetch one row since PIN should be unique
        
        # If no property data is found, return None
        if not property_data:
            st.warning(f"No property found for PIN {pin}.")
            return None, None, None
        
        # Ensure all results are fetched to avoid "Unread result found" error
        if cursor.with_rows:
            cursor.fetchall()  # Fetch and discard any remaining rows if necessary

        # Fetch property details using PropertyId
        cursor.execute("""
            SELECT * FROM property_details 
            WHERE PropertyID = (SELECT PropertyID FROM properties WHERE TRIM(PIN) = %s)
        """, (pin.strip(),))
        property_details_data = cursor.fetchall()  # Fetch all results for details
        
        # If no property details data is found, set it to an empty list
        if not property_details_data:
            property_details_data = []

        # Fetch violations data using PropertyId
        cursor.execute("""
            SELECT * FROM violations 
            WHERE PropertyID = (SELECT PropertyID FROM properties WHERE TRIM(PIN) = %s)
        """, (pin.strip(),))
        violations_data = cursor.fetchall()  # Fetch all results for violations

        # If no violations data is found, set it to an empty list
        if not violations_data:
            violations_data = []

        return property_data, property_details_data, violations_data

    except mysql.connector.Error as err:
        st.error(f"Database error: {err}")
        connection.rollback()
        return None, None, None
    finally:
        cursor.close()
        connection.close()

# Function to remove selected rows
def remove_selected_rows(pin, selected_properties, selected_details, selected_violations):
    connection = authenticate_user(st.session_state.username, st.session_state.password)
    if connection is None:
        st.error("Failed to connect to the database.")
        return

    try:
        cursor = connection.cursor()

        # Remove selected rows from `properties`
        if not selected_properties.empty:
            cursor.execute("DELETE FROM properties WHERE PIN = %s", (pin,))
        
        # Remove selected rows from `property_details`
        for _, row in selected_details.iterrows():
            cursor.execute("DELETE FROM property_details WHERE id = %s", (row['id'],))

        # Remove selected rows from `violations`
        for _, row in selected_violations.iterrows():
            cursor.execute("DELETE FROM violations WHERE id = %s", (row['id'],))

        connection.commit()

    except mysql.connector.Error as err:
        st.error(f"Database error: {err}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


    st.title("Remove Building Data")
    
    # Fetch the building data
    property_data, property_details_data, violations_data = fetch_building_data(pin)
    
    if not property_data:
        st.warning(f"No data found for PIN {pin}")
        return

    st.subheader("Property Information: Select Rows to Remove")
    st.write(property_data)  # Display the basic property info

    # Property Details
    st.subheader("Property Details: Select Rows to Remove")
    if property_details_data:
        property_details_df = pd.DataFrame(property_details_data)
        selected_details_rows = st.multiselect("Select property details to delete", property_details_df.index, format_func=lambda x: f"{property_details_df.at[x, 'DetailKey']}: {property_details_df.at[x, 'DetailValue']}")
    else:
        st.warning("No property details found.")

    # Violations
    st.subheader("Violations: Select Rows to Remove")
    if violations_data:
        violations_df = pd.DataFrame(violations_data)
        selected_violations_rows = st.multiselect("Select violations to delete", violations_df.index, format_func=lambda x: f"{violations_df.at[x, 'violation_code']}: {violations_df.at[x, 'violation_description']}")
    else:
        st.warning("No violations data found.")
    
    if st.button("Delete Selected Data"):
        connection = authenticate_user(st.session_state.username, st.session_state.password)
        if connection is None:
            st.error("Failed to connect to the database.")
            return
        
        try:
            cursor = connection.cursor()

            # Delete selected property details
            if selected_details_rows:
                for row in selected_details_rows:
                    detail_id = property_details_df.at[row, 'id']
                    cursor.execute("DELETE FROM property_details WHERE id = %s", (detail_id,))
            
            # Delete selected violations
            if selected_violations_rows:
                for row in selected_violations_rows:
                    violation_id = violations_df.at[row, 'id']
                    cursor.execute("DELETE FROM violations WHERE id = %s", (violation_id,))
            
            # If user selects no rows for either violations or details, retain them
            connection.commit()
            st.success("Selected data has been successfully deleted.")
        except mysql.connector.Error as err:
            st.error(f"Database error: {err}")
            connection.rollback()
        finally:
            cursor.close()
            connection.close()

def proceed_with_addition(pin):
    property_data = scrape_property_data(pin)
    if property_data:
        violations_data = fetch_api_data(property_data.get('StreetNumber', ''))
        if violations_data:
            violations_df = pd.DataFrame(violations_data)
            st.session_state.property_data = property_data
            st.session_state.violations_data = violations_df
            st.session_state.page = "confirm_submit"
        else:
            st.error("No violations data found. Please add manually.")
    else:
        st.error("No property data found for this PIN.")

def handle_login(username, password):
  connection = authenticate_user(username, password)
  if connection:
      st.session_state.authenticated = True
      st.session_state.username = username
      st.session_state.password = password
      st.session_state.page = "action_choice"
  else:
      st.error("Authentication failed!")

def handle_go_back():
    if "previous_page" in st.session_state:
        st.session_state.page = st.session_state.previous_page
    else:
        st.session_state.page = "action_choice" 

def handle_confirm():
    # Fetch violations data based on the assessor's street number
    violations_data = fetch_api_data(st.session_state.property_data.get('StreetNumber', ''))
    
    if violations_data and len(violations_data) > 0:
        st.session_state.violations_data = pd.DataFrame(violations_data)
    else:
        st.warning("No violations data found. Please manually add violations.")
        st.session_state.violations_data = pd.DataFrame(columns=['violation_code', 'violation_description', 'violation_status', 'violation_date'])

    # Navigate to the API data page
    st.session_state.page = "api_data"

def handle_submit():
    # Ensure both assessor data and violations data are present before submitting
    if st.session_state.property_data and not st.session_state.violations_data.empty:
        # Insert assessor data and violations data into the database
        insert_into_database(
            st.session_state.property_data.get('StreetNumber', ''),
            st.session_state.property_data.get('Pin', ''),
            st.session_state.property_data.get('Owner', ''),
            st.session_state.property_data.get('Address', ''),
            st.session_state.property_data.get('City', ''),
            st.session_state.property_data.get('State', ''),
            st.session_state.property_data.get('ZIP', ''),
            st.session_state.property_data.get('SquareFootage', ''),
            st.session_state.property_data.get('Link', ''),
            st.session_state.violations_data,
            st.session_state.property_data  # Assuming you want to include assessor details
        )
        st.success("Data successfully submitted to the database.")
    else:
        st.error("Please ensure all necessary data is provided before submitting.")

def api_data_page():
    st.title("Additional Data")
    
    # Assessor Data
    st.subheader("Assessor Data")
    assessor_df = pd.DataFrame([st.session_state.property_data], index=["Assessor Data"])
    edited_assessor_df = st.data_editor(assessor_df, num_rows="dynamic")
    st.session_state.property_data = edited_assessor_df.iloc[0].to_dict()

    # Violations Data
    st.subheader("Violations Data")
    if st.session_state.violations_data.empty:
        st.warning("No violations data found. Please enter violations manually.")
    
    # Editable table for violations
    edited_violations_df = st.data_editor(st.session_state.violations_data, num_rows="dynamic")
    st.session_state.violations_data = edited_violations_df

    # Go Back Button
    st.button("Go Back", on_click=handle_go_back)
    
    # Submit Button (optional)
    if st.button("Submit"):
        handle_submit()

def confirm_submit_page():
    st.title("Confirm & Submit General Information")
    
    # Ensure you have the pin in session state
    if "pin" not in st.session_state:
        st.error("PIN not found in session state.")
        return

    property_data = st.session_state.property_data

    # Editable fields for review
    street_number = st.text_input("Street Number", property_data.get('StreetNumber', ''))
    owner = st.text_input("Owner", property_data.get('Owner', ''))
    address = st.text_input("Owner Address", property_data.get('StreetNumber', ''))
    city = st.text_input("City", property_data.get('City', ''))
    state = st.text_input("State", property_data.get('State', ''))
    zip_code = st.text_input("ZIP Code", property_data.get('ZIP', ''))
    square_feet = st.text_input("Square Feet", property_data.get('SquareFootage', ''))
    link = st.text_input("Link", property_data.get('Link', ''))

    # Editable PIN field with default value from session state
    pin = st.text_input("PIN", st.session_state.pin)  # Editable field for the PIN
    st.button("Confirm", on_click=handle_confirm)
    st.button("Go Back", on_click=handle_go_back)

def action_choice_page():
    st.title("Choose Action and Enter Building PIN")
    action = st.radio("Would you like to add or remove a building?", ("Add Building", "Remove Building"))
    pin = st.text_input("Enter the 14-digit Building PIN")
    
    if st.button("Continue"):
        st.session_state.action = action
        st.session_state.pin = pin
        if action == "Add Building":
            proceed_with_addition(pin)
        elif action == "Remove Building":
            remove_from_database(pin)


# Streamlit Pages
def login_page():
    st.title("Building to Business Database Update:")
    st.header("Login")
    st.subheader("Enter your credentials for access to the MySQL Database")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    st.button("Login", on_click=handle_login, args=(username, password))

# Main app logic
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if "page" not in st.session_state:
    st.session_state.page = "login"

if st.session_state.page == "login":
    login_page()
elif st.session_state.page == "action_choice" and st.session_state.authenticated:
    action_choice_page()
elif st.session_state.page == "confirm_submit" and st.session_state.authenticated:
    confirm_submit_page()
elif st.session_state.page == "api_data" and st.session_state.authenticated:
    api_data_page()
elif st.session_state.page == "remove_building" and st.session_state.authenticated:
    remove_building_page()
