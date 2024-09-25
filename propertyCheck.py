import streamlit as st
import mysql.connector
import pandas as pd

# Database connection
def create_connection():
    return mysql.connector.connect(
        host="cbaworks.org",
        user="margaretpirozzolo",
        password="eO!gKo(tIgEY",
        database="vacancy_abatement"
    )

# Function to fetch data based on user's interest
def fetch_data(connection, interest, filters):
    if interest == "Properties":
        query = """
            SELECT p.*, pd.Description, p.link, p.Owner,
            (SELECT COUNT(*) FROM vacancy_abatement.violations v WHERE v.PropertyID = p.PropertyID) AS violation_count
            FROM vacancy_abatement.properties p
            JOIN vacancy_abatement.property_details pd ON p.PropertyID = pd.PropertyId
        """
        conditions = []
        if filters["address"]:
            conditions.append(f"p.Address LIKE '%{filters['address']}%'")
        if filters["min_square_footage"] and filters["max_square_footage"]:
            conditions.append(f"p.SquareFeet BETWEEN {filters['min_square_footage']} AND {filters['max_square_footage']}")
        elif filters["min_square_footage"]:
            conditions.append(f"p.SquareFeet >= {filters['min_square_footage']}")
        elif filters["max_square_footage"]:
            conditions.append(f"p.SquareFeet <= {filters['max_square_footage']}")
        if filters["building_type"]:
            conditions.append(f"pd.Description = '{filters['building_type']}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    return pd.DataFrame(rows)

# Function to fetch violations for a property
def fetch_violations(connection, property_id):
    query = f"""
        SELECT *
        FROM vacancy_abatement.violations
        WHERE PropertyID = {property_id}
    """
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    return pd.DataFrame(rows)

# Function to save user input to the database
def save_user_input(connection, name, email):
    cursor = connection.cursor()
    query = "INSERT INTO visitors (name, email) VALUES (%s, %s)"
    cursor.execute(query, (name, email))
    connection.commit()
    cursor.close()

# Function to save a user's favorite properties
def save_favorite(connection, name, email, property_id):
    cursor = connection.cursor()
    query = """
        INSERT INTO user_favorites (name, email, PropertyID)
        VALUES (%s, %s, %s)
    """
    cursor.execute(query, (name, email, property_id))
    connection.commit()
    cursor.close()

# Function to remove a user's favorite properties
def remove_favorite(connection, name, email, property_id):
    cursor = connection.cursor()
    query = """
        DELETE FROM user_favorites
        WHERE name = %s AND email = %s AND PropertyID = %s
    """
    cursor.execute(query, (name, email, property_id))
    connection.commit()
    cursor.close()

# Function to fetch a user's favorite properties
def fetch_favorites(connection, name, email):
    query = """
        SELECT p.*, pd.Description, p.link, p.Owner,
        (SELECT COUNT(*) FROM vacancy_abatement.violations v WHERE v.PropertyID = p.PropertyID) AS violation_count
        FROM user_favorites uf
        JOIN vacancy_abatement.properties p ON uf.PropertyID = p.PropertyID
        JOIN vacancy_abatement.property_details pd ON p.PropertyID = pd.PropertyId
        WHERE uf.name = %s AND uf.email = %s
    """
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query, (name, email))
    rows = cursor.fetchall()
    return pd.DataFrame(rows)

# Streamlit app
logo_url = "https://www.pbcchicago.com/wp-content/uploads/2018/04/CBA-Logo.jpg"

# Display the logo at the top of the app
st.markdown(
    f"""
    <div style="text-align: center;">
        <img src="{logo_url}" width="100">
    </div>
    """,
    unsafe_allow_html=True
)

st.title("Building to Business Database")

# Initialize session state variables
if "page" not in st.session_state:
    st.session_state.page = 1

if "show_favorites" not in st.session_state:
    st.session_state.show_favorites = False

# Page 1: User input for name and email
if st.session_state.page == 1:
    name = st.text_input("Enter your name")
    email = st.text_input("Enter your email")

    # Save user input and move to the next page
    if st.button("Next"):
        if name and email:
            connection = create_connection()
            save_user_input(connection, name, email)
            connection.close()
            st.session_state.page = 2
            st.session_state.name = name
            st.session_state.email = email
        else:
            st.warning("Please enter both name and email")

# Page 2: Parameters and property listing
if st.session_state.page == 2:
    st.header("Search Properties")

    # Toggle showing favorites
    if st.session_state.show_favorites:
        if st.button("Hide My Favorites"):
            st.session_state.show_favorites = False
    else:
        if st.button("Show My Favorites"):
            st.session_state.show_favorites = True

    # View or hide favorites
    if st.session_state.show_favorites:
        connection = create_connection()
        favorite_properties = fetch_favorites(connection, st.session_state.name, st.session_state.email)
        st.write(favorite_properties)

        for index, row in favorite_properties.iterrows():
            if st.button(f"Remove {row['PropertyID']} from Favorites", key=f"remove_{index}"):
                remove_favorite(connection, st.session_state.name, st.session_state.email, row['PropertyID'])
                st.experimental_set_query_params(dummy=str(index))  # Workaround to refresh the app after removal

        connection.close()

    # User input for filters
    with st.sidebar:
        address = st.text_input("Enter address (optional)")
        min_square_footage = st.number_input("Enter minimum square footage (optional)", min_value=0, value=0)
        max_square_footage = st.number_input("Enter maximum square footage (optional)", min_value=0, value=0)

        building_types = [
            'One story store', 'Two to six apartments, up to 62 years', 'Commercial minor improvements',
            'Mixed use commercial/residential with apts. above seven units or more or building sq. ft. over 20,000',
            'Vacant land', 'Other minor improvement which does not add value',
            'One story non-fireproof public garage',
            'Two or three story building containing part or all retail and/or commercial space',
            'Vacant land under common ownership with adjacent residence', 'Two to six apartments, over 62 years',
            'Special commercial improvements', 'Other minor improvements', 'Gasoline station',
            'Apartment buildings over three stories',
            'One story retail, restaurant, or banquet hall, medical building, miscellaneous commercial use',
            'Bank buildings', 'Other industrial minor improvements',
            'Garage used in conjunction with commercial improvements', 'Not for profit One story store',
            'Two or three story non-frprf. crt. and corridor apts or california type apts, no corridors, ex. entrance',
            'Motels', 'Residential garage'
        ]
        building_type = st.selectbox("Select building type (optional)", options=building_types)

        filters = {
            "address": address,
            "min_square_footage": min_square_footage if min_square_footage > 0 else None,
            "max_square_footage": max_square_footage if max_square_footage > 0 else None,
            "building_type": building_type
        }

    # Display properties
    connection = create_connection()
    data = fetch_data(connection, "Properties", filters)

    st.subheader("Properties List")

    for index, row in data.iterrows():
        with st.container():
            st.markdown(
                f"""
                <div style="border: 1px solid #ddd; padding: 10px; margin-bottom: 10px;">
                    <h4><a href="{row['link']}">{row['Address']}</a></h4>
                    <p>{row['SquareFeet']} sqft, {row['Description']}</p>
                    <p>Owner: {row['Owner']}</p>
                """, unsafe_allow_html=True)

            if row['violation_count'] > 0:
                if st.button(f"Show {row['violation_count']} Violations", key=f"violation_{index}"):
                    violations = fetch_violations(connection, row['PropertyID'])
                    st.markdown("<h5>Violations:</h5>", unsafe_allow_html=True)
                    st.write(violations)
            else:
                st.markdown("<p>No violations on record for this property</p>", unsafe_allow_html=True)

            if st.button("Save to Favorites", key=f"save_{index}"):
                save_favorite(connection, st.session_state.name, st.session_state.email, row['PropertyID'])
                st.success("Property saved to favorites")

            st.markdown("</div>", unsafe_allow_html=True)

    connection.close()

# Add a disclaimer at the bottom
st.markdown(
    """
    <div style="margin-top: 20px; font-size: small; text-align: left; color: gray;">
        Disclaimer: The information provided herein is sourced from publicly available data and is subject to change.
        Users are advised to independently verify all details before taking any action. The Chicago Business Alliance (CBA)
        assumes no responsibility for the accuracy, completeness, or timeliness of the information provided, and shall not
        be liable for any decisions made based on this information.
    </div>
    """,
    unsafe_allow_html=True
)
