"""
Smart Location & Metro Expansion — Location Hierarchy Data

Hierarchical location dictionary:
    Continent -> Country -> Region/Province -> Base City -> Metro Sub-cities

Helper functions:
    get_continents()
    get_countries_by_continent(continent)
    get_countries()
    get_regions(country)
    get_base_cities(country, region)
    get_sub_cities(country, region, city)
    get_all_cities_flat(country, region)
    is_known_location(country, region, city)
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Continent → Country grouping
# ---------------------------------------------------------------------------

CONTINENT_COUNTRIES: dict[str, list[str]] = {
    "Africa": [
        "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi",
        "Cabo Verde", "Cameroon", "Central African Republic", "Chad", "Comoros",
        "Congo", "Djibouti", "Egypt", "Equatorial Guinea", "Eritrea", "Eswatini",
        "Ethiopia", "Gabon", "Gambia", "Ghana", "Guinea", "Guinea-Bissau",
        "Kenya", "Lesotho", "Liberia", "Libya", "Madagascar", "Malawi", "Mali",
        "Mauritania", "Mauritius", "Morocco", "Mozambique", "Namibia", "Niger",
        "Nigeria", "Rwanda", "Sao Tome and Principe", "Senegal", "Seychelles",
        "Sierra Leone", "Somalia", "South Africa", "South Sudan", "Sudan",
        "Tanzania", "Togo", "Tunisia", "Uganda", "Zambia", "Zimbabwe",
    ],
    "Asia": [
        "Afghanistan", "Bangladesh", "Bhutan", "Brunei", "Cambodia", "China",
        "India", "Indonesia", "Japan", "Kazakhstan", "Kyrgyzstan", "Laos",
        "Malaysia", "Maldives", "Mongolia", "Myanmar", "Nepal", "North Korea",
        "Pakistan", "Philippines", "Singapore", "South Korea", "Sri Lanka",
        "Taiwan", "Tajikistan", "Thailand", "Timor-Leste", "Turkmenistan",
        "Uzbekistan", "Vietnam",
    ],
    "Europe": [
        "Albania", "Andorra", "Armenia", "Austria", "Azerbaijan", "Belarus",
        "Belgium", "Bosnia and Herzegovina", "Bulgaria", "Croatia", "Cyprus",
        "Czech Republic", "Denmark", "Estonia", "Finland", "France", "Georgia",
        "Germany", "Greece", "Hungary", "Iceland", "Ireland", "Italy",
        "Latvia", "Liechtenstein", "Lithuania", "Luxembourg", "Malta", "Moldova",
        "Monaco", "Montenegro", "Netherlands", "North Macedonia", "Norway",
        "Poland", "Portugal", "Romania", "Russia", "San Marino", "Serbia",
        "Slovakia", "Slovenia", "Spain", "Sweden", "Switzerland", "Ukraine",
        "United Kingdom", "Vatican City",
    ],
    "Middle East": [
        "Bahrain", "Iran", "Iraq", "Israel", "Jordan", "Kuwait", "Lebanon",
        "Oman", "Palestine", "Qatar", "Saudi Arabia", "Syria",
        "United Arab Emirates", "Yemen",
    ],
    "North America": [
        "Antigua and Barbuda", "Bahamas", "Barbados", "Belize", "Canada",
        "Costa Rica", "Cuba", "Dominica", "Dominican Republic", "El Salvador",
        "Grenada", "Guatemala", "Haiti", "Honduras", "Jamaica", "Mexico",
        "Nicaragua", "Panama", "Saint Kitts and Nevis", "Saint Lucia",
        "Saint Vincent and the Grenadines", "Trinidad and Tobago", "United States",
    ],
    "Oceania": [
        "Australia", "Fiji", "Kiribati", "Marshall Islands", "Micronesia",
        "Nauru", "New Zealand", "Palau", "Papua New Guinea", "Samoa",
        "Solomon Islands", "Tonga", "Tuvalu", "Vanuatu",
    ],
    "South America": [
        "Argentina", "Bolivia", "Brazil", "Chile", "Colombia", "Ecuador",
        "Guyana", "Paraguay", "Peru", "Suriname", "Uruguay", "Venezuela",
    ],
}

# ---------------------------------------------------------------------------
# Full location hierarchy: Country -> Region -> City -> [Sub-cities]
# ---------------------------------------------------------------------------

LOCATION_HIERARCHY: dict[str, dict[str, dict[str, list[str]]]] = {

    # =========================================================================
    # NORTH AMERICA
    # =========================================================================

    "Canada": {
        "British Columbia": {
            "Vancouver": ["Burnaby", "Richmond", "Surrey", "Coquitlam",
                          "Langley", "Delta", "New Westminster", "North Vancouver"],
            "Victoria": ["Saanich", "Langford", "Oak Bay", "Esquimalt", "Colwood"],
            "Kelowna": ["West Kelowna", "Peachland", "Penticton", "Vernon"],
        },
        "Ontario": {
            "Toronto": ["Mississauga", "Brampton", "Markham", "Vaughan",
                        "Oakville", "Burlington", "Ajax", "Whitby", "Pickering"],
            "Ottawa": ["Kanata", "Nepean", "Orleans", "Gloucester"],
            "Hamilton": ["Burlington", "Stoney Creek", "Brantford", "Grimsby"],
            "London": ["Sarnia", "Windsor", "Chatham-Kent", "Woodstock"],
        },
        "Alberta": {
            "Calgary": ["Airdrie", "Cochrane", "Okotoks", "Chestermere",
                        "Strathmore", "High River"],
            "Edmonton": ["St. Albert", "Sherwood Park", "Leduc",
                         "Fort Saskatchewan", "Spruce Grove", "Stony Plain"],
        },
        "Quebec": {
            "Montreal": ["Laval", "Longueuil", "Brossard", "Boucherville",
                         "Saint-Jerome", "Terrebonne", "Repentigny"],
            "Quebec City": ["Levis", "Sillery", "Charlesbourg", "Sainte-Foy"],
            # National Capital Region spans Ontario (Ottawa) and Quebec (Gatineau)
            "Gatineau": ["Aylmer", "Hull", "Buckingham", "Cantley"],
        },
        "Manitoba": {
            "Winnipeg": ["St. Vital", "Transcona", "St. James", "Tuxedo"],
        },
        "Saskatchewan": {
            "Saskatoon": ["Martensville", "Warman", "Corman Park"],
            "Regina": ["Moose Jaw", "Weyburn", "Estevan"],
        },
        "Nova Scotia": {
            "Halifax": ["Dartmouth", "Bedford", "Sackville", "Truro"],
        },
        "New Brunswick": {
            "Moncton": ["Fredericton", "Saint John", "Dieppe", "Riverview"],
        },
    },

    "United States": {
        "Texas": {
            "Dallas": ["Fort Worth", "Arlington", "Plano", "Garland", "Irving",
                       "Frisco", "McKinney", "Carrollton"],
            "Houston": ["Pasadena", "Sugar Land", "Pearland", "The Woodlands",
                        "Baytown", "League City", "Katy", "Missouri City"],
            "Austin": ["Round Rock", "Cedar Park", "Georgetown", "San Marcos",
                       "Kyle", "Pflugerville", "Leander"],
            "San Antonio": ["New Braunfels", "Schertz", "Converse", "Universal City"],
        },
        "California": {
            "Los Angeles": ["Long Beach", "Anaheim", "Pasadena", "Glendale",
                            "Burbank", "Santa Monica", "Torrance", "Pomona"],
            "San Diego": ["Chula Vista", "Oceanside", "Carlsbad", "El Cajon",
                          "Escondido", "Vista", "San Marcos"],
            "San Jose": ["Sunnyvale", "Santa Clara", "Fremont", "Palo Alto",
                         "Mountain View", "Milpitas", "Cupertino"],
            "San Francisco": ["Oakland", "Berkeley", "San Mateo", "Daly City",
                              "South San Francisco", "Redwood City"],
            "Sacramento": ["Elk Grove", "Roseville", "Folsom", "Rancho Cordova",
                           "Citrus Heights", "Woodland"],
            "Fresno": ["Clovis", "Madera", "Visalia", "Tulare"],
        },
        "Florida": {
            "Miami": ["Fort Lauderdale", "Hollywood", "Hialeah", "Pembroke Pines",
                      "Miramar", "Coral Springs", "Pompano Beach"],
            "Orlando": ["Kissimmee", "Sanford", "Winter Park", "Apopka",
                        "Deltona", "Ocoee", "Altamonte Springs"],
            "Tampa": ["St. Petersburg", "Clearwater", "Brandon", "Lakeland",
                      "Largo", "Palm Harbor", "Wesley Chapel"],
            "Jacksonville": ["Orange Park", "Fleming Island", "Ponte Vedra", "St. Augustine"],
        },
        "Arizona": {
            "Phoenix": ["Scottsdale", "Tempe", "Mesa", "Chandler", "Gilbert",
                        "Peoria", "Surprise", "Glendale", "Goodyear"],
            "Tucson": ["Marana", "Sahuarita", "Oro Valley", "Sierra Vista"],
        },
        "Nevada": {
            "Las Vegas": ["Henderson", "North Las Vegas", "Boulder City",
                          "Summerlin", "Paradise", "Enterprise"],
            "Reno": ["Sparks", "Carson City", "Fernley"],
        },
        "Colorado": {
            "Denver": ["Aurora", "Lakewood", "Thornton", "Westminster",
                       "Arvada", "Englewood", "Centennial", "Littleton"],
            "Colorado Springs": ["Pueblo", "Fountain", "Monument"],
        },
        "Washington": {
            "Seattle": ["Bellevue", "Redmond", "Kirkland", "Renton",
                        "Tacoma", "Everett", "Bothell", "Kent"],
        },
        "New York": {
            "New York City": ["Brooklyn", "Queens", "Bronx", "Staten Island",
                              "Yonkers", "New Rochelle", "White Plains"],
            "Buffalo": ["Amherst", "Cheektowaga", "Tonawanda", "Lackawanna"],
        },
        "North Carolina": {
            "Charlotte": ["Concord", "Gastonia", "Rock Hill", "Huntersville",
                          "Matthews", "Kannapolis", "Mooresville"],
            "Raleigh": ["Durham", "Chapel Hill", "Cary", "Apex",
                        "Morrisville", "Wake Forest", "Garner"],
        },
        "Georgia": {
            "Atlanta": ["Marietta", "Alpharetta", "Sandy Springs", "Roswell",
                        "Decatur", "Johns Creek", "Smyrna", "Peachtree City"],
        },
        "Virginia": {
            "Northern Virginia": ["Arlington", "Alexandria", "Fairfax", "Reston",
                                  "Herndon", "Sterling", "Ashburn"],
            "Richmond": ["Chesterfield", "Henrico", "Chester", "Colonial Heights"],
        },
        "Illinois": {
            "Chicago": ["Evanston", "Schaumburg", "Naperville", "Aurora",
                        "Joliet", "Elgin", "Waukegan", "Cicero"],
        },
        "New Jersey": {
            "Newark": ["Jersey City", "Paterson", "Elizabeth", "Trenton",
                       "Edison", "Woodbridge", "Parsippany"],
        },
        "Massachusetts": {
            "Boston": ["Cambridge", "Worcester", "Springfield", "Lowell",
                       "Newton", "Quincy", "Waltham"],
        },
        "Oregon": {
            "Portland": ["Beaverton", "Gresham", "Hillsboro", "Lake Oswego",
                         "Tigard", "Tualatin", "Salem"],
        },
        "Minnesota": {
            "Minneapolis": ["Saint Paul", "Bloomington", "Plymouth", "Brooklyn Park",
                            "Edina", "Eden Prairie", "Minnetonka"],
        },
        "Ohio": {
            "Columbus": ["Dublin", "Westerville", "Delaware", "Hilliard",
                         "Grove City", "Gahanna", "Reynoldsburg"],
            "Cleveland": ["Akron", "Elyria", "Strongsville", "Parma", "Lakewood"],
        },
        "Michigan": {
            "Detroit": ["Dearborn", "Ann Arbor", "Livonia", "Sterling Heights",
                        "Warren", "Flint", "Lansing"],
        },
        "Pennsylvania": {
            "Philadelphia": ["Camden", "Wilmington", "Chester", "Norristown",
                             "King of Prussia", "Bensalem"],
            "Pittsburgh": ["Allentown", "Erie", "Bethlehem", "Harrisburg"],
        },
        "Tennessee": {
            "Nashville": ["Murfreesboro", "Franklin", "Hendersonville",
                          "Brentwood", "Smyrna", "Gallatin"],
            "Memphis": ["Germantown", "Bartlett", "Collierville", "Jackson"],
        },
        "Missouri": {
            "Kansas City": ["Independence", "Lee's Summit", "Overland Park",
                            "Olathe", "St. Joseph"],
            "St. Louis": ["Clayton", "Florissant", "Chesterfield", "O'Fallon"],
        },
        "Utah": {
            "Salt Lake City": ["Provo", "Ogden", "West Valley City",
                               "Orem", "Sandy", "St. George"],
        },
        "Hawaii": {
            "Honolulu": ["Pearl City", "Hilo", "Kailua", "Kaneohe",
                         "Waipahu", "Kahului"],
        },
    },

    "Mexico": {
        "Mexico City": {
            "Mexico City": ["Ecatepec", "Nezahualcóyotl", "Tlalnepantla",
                            "Naucalpan", "Guadalupe", "Iztapalapa"],
        },
        "Jalisco": {
            "Guadalajara": ["Zapopan", "Tlaquepaque", "Tonalá", "Tlajomulco",
                            "Puerto Vallarta"],
        },
        "Nuevo León": {
            "Monterrey": ["San Nicolás de los Garza", "Guadalupe", "Apodaca",
                          "San Pedro Garza García", "Escobedo"],
        },
        "Baja California": {
            "Tijuana": ["Mexicali", "Ensenada", "Rosarito", "Tecate"],
        },
        "Puebla": {
            "Puebla": ["Tehuacán", "San Andrés Cholula", "San Martín Texmelucan"],
        },
        "Yucatán": {
            "Mérida": ["Progreso", "Valladolid", "Tizimín", "Umán"],
        },
        "Sonora": {
            "Hermosillo": ["Ciudad Obregón", "Nogales", "Guaymas", "Navojoa"],
        },
        "Chihuahua": {
            "Chihuahua": ["Ciudad Juárez", "Delicias", "Cuauhtémoc", "Parral"],
        },
        "Veracruz": {
            "Veracruz": ["Xalapa", "Coatzacoalcos", "Córdoba", "Orizaba"],
        },
        "Guanajuato": {
            "León": ["Guanajuato", "Irapuato", "Celaya", "Salamanca"],
        },
        "Sinaloa": {
            "Culiacán": ["Mazatlán", "Los Mochis", "Guasave"],
        },
        "Tamaulipas": {
            "Reynosa": ["Matamoros", "Nuevo Laredo", "Tampico", "Victoria"],
        },
    },

    # =========================================================================
    # SOUTH AMERICA
    # =========================================================================

    "Brazil": {
        "São Paulo": {
            "São Paulo": ["Guarulhos", "Campinas", "São Bernardo do Campo",
                          "Santo André", "Osasco", "Ribeirão Preto", "Sorocaba"],
        },
        "Rio de Janeiro": {
            "Rio de Janeiro": ["Niterói", "São Gonçalo", "Duque de Caxias",
                               "Nova Iguaçu", "Belford Roxo", "Petrópolis"],
        },
        "Minas Gerais": {
            "Belo Horizonte": ["Contagem", "Uberlândia", "Uberaba", "Juiz de Fora",
                               "Betim", "Montes Claros"],
        },
        "Bahia": {
            "Salvador": ["Feira de Santana", "Vitória da Conquista",
                         "Camaçari", "Ilhéus"],
        },
        "Rio Grande do Sul": {
            "Porto Alegre": ["Caxias do Sul", "Pelotas", "Canoas",
                             "Santa Maria", "Novo Hamburgo"],
        },
        "Paraná": {
            "Curitiba": ["Londrina", "Maringá", "Foz do Iguaçu",
                         "Ponta Grossa", "São José dos Pinhais"],
        },
        "Pernambuco": {
            "Recife": ["Olinda", "Caruaru", "Petrolina", "Jaboatão dos Guararapes"],
        },
        "Ceará": {
            "Fortaleza": ["Caucaia", "Juazeiro do Norte", "Maracanaú", "Sobral"],
        },
        "Pará": {
            "Belém": ["Ananindeua", "Santarém", "Marabá", "Castanhal"],
        },
        "Goiás": {
            "Goiânia": ["Aparecida de Goiânia", "Anápolis", "Rio Verde"],
        },
        "Amazonas": {
            "Manaus": ["Parintins", "Itacoatiara", "Coari"],
        },
        "Mato Grosso do Sul": {
            "Campo Grande": ["Dourados", "Três Lagoas", "Corumbá"],
        },
    },

    "Argentina": {
        "Buenos Aires": {
            "Buenos Aires": ["La Plata", "Mar del Plata", "Quilmes",
                             "Lanús", "General San Martín", "Lomas de Zamora"],
        },
        "Córdoba": {
            "Córdoba": ["Villa Carlos Paz", "Río Cuarto", "San Francisco", "Río Tercero"],
        },
        "Santa Fe": {
            "Rosario": ["Santa Fe", "Rafaela", "Venado Tuerto", "Villa Gobernador Gálvez"],
        },
        "Mendoza": {
            "Mendoza": ["San Rafael", "Godoy Cruz", "Luján de Cuyo", "Maipú"],
        },
        "Tucumán": {
            "San Miguel de Tucumán": ["Tafí Viejo", "Banda del Río Salí", "Concepción"],
        },
        "Neuquén": {
            "Neuquén": ["Cipolletti", "Plottier", "Centenario", "Zapala"],
        },
        "Chubut": {
            "Comodoro Rivadavia": ["Rawson", "Trelew", "Puerto Madryn"],
        },
    },

    "Chile": {
        "Metropolitan Region": {
            "Santiago": ["Puente Alto", "Maipú", "La Florida", "Las Condes",
                         "Peñalolén", "Lo Barnechea", "San Bernardo"],
        },
        "Valparaíso Region": {
            "Valparaíso": ["Viña del Mar", "Quilpué", "Villa Alemana", "Quillota"],
        },
        "Biobío Region": {
            "Concepción": ["Talcahuano", "Chiguayante", "San Pedro de la Paz",
                           "Coronel", "Hualpén"],
        },
        "Araucanía Region": {
            "Temuco": ["Padre Las Casas", "Villarrica", "Pucón", "Angol"],
        },
        "Antofagasta Region": {
            "Antofagasta": ["Calama", "Tocopilla", "Mejillones"],
        },
        "Los Lagos Region": {
            "Puerto Montt": ["Puerto Varas", "Osorno", "Castro", "Ancud"],
        },
    },

    "Colombia": {
        "Bogotá D.C.": {
            "Bogotá": ["Soacha", "Chía", "Zipaquirá", "Facatativá", "Mosquera"],
        },
        "Antioquia": {
            "Medellín": ["Bello", "Itagüí", "Envigado", "Rionegro",
                         "Sabaneta", "La Estrella"],
        },
        "Valle del Cauca": {
            "Cali": ["Palmira", "Buenaventura", "Tuluá", "Buga", "Cartago"],
        },
        "Atlántico": {
            "Barranquilla": ["Soledad", "Malambo", "Puerto Colombia", "Galapa"],
        },
        "Santander": {
            "Bucaramanga": ["Floridablanca", "Girón", "Piedecuesta", "Lebrija"],
        },
        "Bolívar": {
            "Cartagena": ["Turbaco", "Arjona", "Magangué"],
        },
        "Cundinamarca": {
            "Villavicencio": ["Acacías", "Granada", "Puerto López"],
        },
    },

    "Peru": {
        "Lima Region": {
            "Lima": ["Callao", "San Juan de Lurigancho", "La Molina",
                     "Miraflores", "Surco", "Villa El Salvador"],
        },
        "Arequipa": {
            "Arequipa": ["Cayma", "Cerro Colorado", "Socabaya", "Yura"],
        },
        "La Libertad": {
            "Trujillo": ["La Esperanza", "El Porvenir", "Florencia de Mora", "Huanchaco"],
        },
        "Piura": {
            "Piura": ["Castilla", "Sullana", "Talara", "Paita"],
        },
        "Cusco": {
            "Cusco": ["San Jerónimo", "San Sebastián", "Wanchaq", "Urubamba"],
        },
        "Junín": {
            "Huancayo": ["El Tambo", "Chilca", "San Agustín de Cajas"],
        },
    },

    "Venezuela": {
        "Capital District": {
            "Caracas": ["Petare", "El Hatillo", "Chacao", "Baruta", "Sucre"],
        },
        "Miranda": {
            "Los Teques": ["Guarenas", "Guatire", "Ocumare del Tuy", "Charallave"],
        },
        "Zulia": {
            "Maracaibo": ["San Francisco", "Cabimas", "Ciudad Ojeda", "Lagunillas"],
        },
        "Carabobo": {
            "Valencia": ["Maracay", "Guacara", "Los Guayos", "Puerto Cabello"],
        },
        "Lara": {
            "Barquisimeto": ["Cabudare", "El Tocuyo", "Quíbor", "Carora"],
        },
    },

    "Ecuador": {
        "Pichincha": {
            "Quito": ["Sangolquí", "Cayambe", "Machachi", "Mejía"],
        },
        "Guayas": {
            "Guayaquil": ["Durán", "Samborondón", "Milagro", "Daule"],
        },
        "Azuay": {
            "Cuenca": ["Gualaceo", "Paute", "Sigsig", "Chordeleg"],
        },
        "Manabí": {
            "Portoviejo": ["Manta", "Chone", "El Carmen", "Jipijapa"],
        },
    },

    "Bolivia": {
        "Santa Cruz": {
            "Santa Cruz de la Sierra": ["Warnes", "Montero", "La Guardia", "Cotoca"],
        },
        "La Paz": {
            "La Paz": ["El Alto", "Viacha", "Achocalla", "Mecapaca"],
        },
        "Cochabamba": {
            "Cochabamba": ["Quillacollo", "Sacaba", "Tiquipaya", "Colcapirhua"],
        },
        "Oruro": {
            "Oruro": ["Caracollo", "Huanuni", "Machacamarca"],
        },
    },

    "Uruguay": {
        "Montevideo": {
            "Montevideo": ["Ciudad de la Costa", "Las Piedras", "La Paz", "Barros Blancos"],
        },
        "Canelones": {
            "Canelones": ["Pando", "Progreso", "Atlántida", "Salinas"],
        },
        "Maldonado": {
            "Maldonado": ["Punta del Este", "San Carlos", "Piriápolis"],
        },
    },

    "Paraguay": {
        "Central": {
            "Asunción": ["Fernando de la Mora", "Lambaré", "Luque",
                         "San Lorenzo", "Capiatá", "Mariano Roque Alonso"],
        },
        "Alto Paraná": {
            "Ciudad del Este": ["Presidente Franco", "Hernandarias", "Minga Guazú"],
        },
        "Itapúa": {
            "Encarnación": ["Fram", "Coronel Bogado", "General Delgado"],
        },
    },

    "Guyana": {
        "Demerara-Mahaica": {
            "Georgetown": ["Linden", "New Amsterdam", "Anna Regina"],
        },
    },

    # =========================================================================
    # EUROPE
    # =========================================================================

    "United Kingdom": {
        "England": {
            "London": ["Croydon", "Bromley", "Ealing", "Barnet",
                       "Hounslow", "Kingston upon Thames", "Sutton"],
            "Manchester": ["Salford", "Bolton", "Stockport", "Oldham",
                           "Rochdale", "Wigan", "Bury"],
            "Birmingham": ["Coventry", "Wolverhampton", "Walsall",
                           "Dudley", "Solihull", "Sandwell"],
            "Leeds": ["Bradford", "Wakefield", "Harrogate", "Huddersfield",
                      "Halifax", "Dewsbury"],
            "Bristol": ["Bath", "Gloucester", "Swindon", "Cheltenham",
                        "Weston-super-Mare"],
            "Liverpool": ["Birkenhead", "Knowsley", "Sefton", "St Helens", "Wirral"],
            "Sheffield": ["Rotherham", "Barnsley", "Doncaster", "Chesterfield"],
            "Newcastle": ["Sunderland", "Gateshead", "Middlesbrough",
                          "Darlington", "Durham"],
        },
        "Scotland": {
            "Edinburgh": ["Leith", "Musselburgh", "Livingston",
                          "Dalkeith", "Penicuik"],
            "Glasgow": ["Paisley", "East Kilbride", "Hamilton",
                        "Motherwell", "Airdrie", "Cumbernauld"],
            "Aberdeen": ["Inverness", "Dundee", "Perth", "Dunfermline"],
        },
        "Wales": {
            "Cardiff": ["Newport", "Swansea", "Barry", "Caerphilly",
                        "Bridgend", "Neath"],
        },
        "Northern Ireland": {
            "Belfast": ["Lisburn", "Newtownabbey", "Bangor",
                        "Londonderry", "Antrim", "Newry"],
        },
    },

    "Germany": {
        "Bavaria": {
            "Munich": ["Augsburg", "Nuremberg", "Ingolstadt",
                       "Regensburg", "Landshut", "Erlangen"],
        },
        "North Rhine-Westphalia": {
            "Cologne": ["Düsseldorf", "Dortmund", "Essen", "Duisburg",
                        "Bonn", "Bochum", "Wuppertal", "Bielefeld"],
        },
        "Baden-Württemberg": {
            "Stuttgart": ["Karlsruhe", "Freiburg", "Heidelberg",
                          "Mannheim", "Ulm", "Heilbronn"],
        },
        "Berlin": {
            "Berlin": ["Potsdam", "Brandenburg an der Havel",
                       "Cottbus", "Oranienburg"],
        },
        "Hamburg": {
            "Hamburg": ["Kiel", "Lübeck", "Rostock", "Schwerin",
                        "Flensburg", "Neumünster"],
        },
        "Hesse": {
            "Frankfurt": ["Wiesbaden", "Darmstadt", "Offenbach",
                          "Kassel", "Hanau", "Gießen"],
        },
        "Lower Saxony": {
            "Hanover": ["Brunswick", "Osnabrück", "Wolfsburg",
                        "Oldenburg", "Göttingen", "Hildesheim"],
        },
        "Saxony": {
            "Dresden": ["Leipzig", "Chemnitz", "Zwickau", "Görlitz"],
        },
        "Rhine-Palatinate": {
            "Mainz": ["Ludwigshafen", "Kaiserslautern", "Trier", "Koblenz"],
        },
        "Saxony-Anhalt": {
            "Magdeburg": ["Halle", "Dessau-Roßlau", "Lutherstadt Wittenberg"],
        },
    },

    "France": {
        "Île-de-France": {
            "Paris": ["Boulogne-Billancourt", "Saint-Denis", "Argenteuil",
                      "Versailles", "Nanterre", "Créteil", "Montreuil"],
        },
        "Auvergne-Rhône-Alpes": {
            "Lyon": ["Villeurbanne", "Grenoble", "Saint-Étienne",
                     "Clermont-Ferrand", "Chambéry", "Annecy"],
        },
        "Provence-Alpes-Côte d'Azur": {
            "Marseille": ["Aix-en-Provence", "Nice", "Toulon",
                          "Avignon", "Cannes", "Antibes"],
        },
        "Occitanie": {
            "Toulouse": ["Montpellier", "Nîmes", "Perpignan",
                         "Narbonne", "Béziers"],
        },
        "Hauts-de-France": {
            "Lille": ["Roubaix", "Tourcoing", "Amiens",
                      "Dunkirk", "Valenciennes"],
        },
        "Nouvelle-Aquitaine": {
            "Bordeaux": ["Mérignac", "Pessac", "Bayonne",
                         "Pau", "Limoges", "Poitiers"],
        },
        "Pays de la Loire": {
            "Nantes": ["Saint-Nazaire", "Le Mans", "Angers", "La Roche-sur-Yon"],
        },
        "Grand Est": {
            "Strasbourg": ["Mulhouse", "Reims", "Nancy", "Metz", "Colmar"],
        },
    },

    "Italy": {
        "Lombardy": {
            "Milan": ["Monza", "Bergamo", "Brescia", "Varese",
                      "Como", "Lecco", "Pavia"],
        },
        "Lazio": {
            "Rome": ["Fiumicino", "Guidonia Montecelio", "Civitavecchia",
                     "Tivoli", "Velletri"],
        },
        "Campania": {
            "Naples": ["Salerno", "Giugliano in Campania", "Torre del Greco",
                       "Caserta", "Pozzuoli"],
        },
        "Sicily": {
            "Palermo": ["Catania", "Messina", "Ragusa", "Siracusa",
                        "Trapani", "Agrigento"],
        },
        "Veneto": {
            "Venice": ["Verona", "Padua", "Vicenza", "Treviso", "Mestre"],
        },
        "Piedmont": {
            "Turin": ["Alessandria", "Asti", "Novara", "Vercelli", "Cuneo"],
        },
        "Emilia-Romagna": {
            "Bologna": ["Modena", "Reggio Emilia", "Parma",
                        "Ferrara", "Rimini", "Forlì"],
        },
        "Tuscany": {
            "Florence": ["Prato", "Livorno", "Arezzo", "Pisa",
                         "Siena", "Grosseto"],
        },
        "Puglia": {
            "Bari": ["Taranto", "Foggia", "Lecce", "Brindisi", "Andria"],
        },
        "Calabria": {
            "Reggio Calabria": ["Catanzaro", "Cosenza", "Crotone", "Vibo Valentia"],
        },
    },

    "Spain": {
        "Community of Madrid": {
            "Madrid": ["Móstoles", "Alcalá de Henares", "Fuenlabrada",
                       "Leganés", "Getafe", "Alcorcón", "Torrejón de Ardoz"],
        },
        "Catalonia": {
            "Barcelona": ["L'Hospitalet de Llobregat", "Badalona", "Terrassa",
                          "Sabadell", "Mataró", "Santa Coloma de Gramenet"],
        },
        "Andalusia": {
            "Seville": ["Málaga", "Córdoba", "Granada", "Jerez de la Frontera",
                        "Almería", "Huelva", "Cádiz"],
        },
        "Valencia": {
            "Valencia": ["Alicante", "Elche", "Castellón de la Plana",
                         "Torrent", "Orihuela"],
        },
        "Basque Country": {
            "Bilbao": ["San Sebastián", "Vitoria-Gasteiz", "Barakaldo",
                       "Getxo", "Irún"],
        },
        "Castile and León": {
            "Valladolid": ["Burgos", "Salamanca", "León", "Palencia", "Ávila"],
        },
        "Galicia": {
            "Vigo": ["A Coruña", "Ourense", "Pontevedra", "Santiago de Compostela"],
        },
        "Canary Islands": {
            "Las Palmas de Gran Canaria": ["Santa Cruz de Tenerife", "La Laguna",
                                           "Arucas", "Telde"],
        },
    },

    "Netherlands": {
        "North Holland": {
            "Amsterdam": ["Haarlem", "Alkmaar", "Zaandam", "Hilversum",
                          "Amstelveen", "Purmerend"],
        },
        "South Holland": {
            "Rotterdam": ["The Hague", "Leiden", "Dordrecht", "Delft",
                          "Zoetermeer", "Westland"],
        },
        "North Brabant": {
            "Eindhoven": ["Tilburg", "Breda", "'s-Hertogenbosch",
                          "Helmond", "Bergen op Zoom"],
        },
        "Utrecht": {
            "Utrecht": ["Amersfoort", "Nieuwegein", "Zeist", "Houten"],
        },
        "Gelderland": {
            "Arnhem": ["Nijmegen", "Apeldoorn", "Ede", "Zwolle"],
        },
        "Groningen": {
            "Groningen": ["Leeuwarden", "Assen", "Emmen", "Hoogeveen"],
        },
    },

    "Belgium": {
        "Brussels": {
            "Brussels": ["Ixelles", "Schaerbeek", "Molenbeek-Saint-Jean",
                         "Anderlecht", "Etterbeek"],
        },
        "Antwerp": {
            "Antwerp": ["Ghent", "Bruges", "Mechelen", "Lier", "Herentals"],
        },
        "East Flanders": {
            "Ghent": ["Aalst", "Sint-Niklaas", "Dendermonde", "Lokeren"],
        },
        "Liège": {
            "Liège": ["Charleroi", "Namur", "Mons", "La Louvière", "Verviers"],
        },
    },

    "Switzerland": {
        "Zurich": {
            "Zurich": ["Winterthur", "Uster", "Dübendorf", "Dietikon", "Bülach"],
        },
        "Bern": {
            "Bern": ["Biel/Bienne", "Thun", "Köniz", "Solothurn"],
        },
        "Geneva": {
            "Geneva": ["Lausanne", "Sion", "Carouge", "Lancy"],
        },
        "Basel-Stadt": {
            "Basel": ["Allschwil", "Binningen", "Riehen", "Muttenz"],
        },
        "Ticino": {
            "Lugano": ["Bellinzona", "Locarno", "Mendrisio", "Chiasso"],
        },
    },

    "Austria": {
        "Vienna": {
            "Vienna": ["Wiener Neustadt", "Klosterneuburg", "Schwechat",
                       "Mödling", "Baden"],
        },
        "Upper Austria": {
            "Linz": ["Wels", "Steyr", "Leonding", "Traun"],
        },
        "Styria": {
            "Graz": ["Leoben", "Kapfenberg", "Bruck an der Mur", "Knittelfeld"],
        },
        "Salzburg": {
            "Salzburg": ["Hallein", "Wals-Siezenheim", "Seekirchen", "Saalfelden"],
        },
        "Tyrol": {
            "Innsbruck": ["Hall in Tirol", "Telfs", "Kufstein", "Imst"],
        },
    },

    "Sweden": {
        "Stockholm County": {
            "Stockholm": ["Solna", "Sundbyberg", "Nacka", "Huddinge",
                          "Botkyrka", "Haninge", "Täby"],
        },
        "Västra Götaland": {
            "Gothenburg": ["Mölndal", "Borås", "Trollhättan", "Alingsås",
                           "Skövde", "Lidköping"],
        },
        "Skåne": {
            "Malmö": ["Lund", "Helsingborg", "Kristianstad", "Landskrona",
                      "Trelleborg"],
        },
        "Uppsala County": {
            "Uppsala": ["Enköping", "Östhammar", "Tierp"],
        },
        "Östergötland": {
            "Linköping": ["Norrköping", "Motala", "Mjölby"],
        },
    },

    "Norway": {
        "Oslo": {
            "Oslo": ["Bærum", "Lillestrøm", "Drammen", "Lørenskog",
                     "Ski", "Asker"],
        },
        "Vestland": {
            "Bergen": ["Åsane", "Fana", "Ytrebygda", "Arna"],
        },
        "Rogaland": {
            "Stavanger": ["Sandnes", "Haugesund", "Randaberg", "Sola"],
        },
        "Trøndelag": {
            "Trondheim": ["Stjørdal", "Melhus", "Malvik", "Klæbu"],
        },
        "Innlandet": {
            "Lillehammer": ["Hamar", "Gjøvik", "Kongsvinger", "Elverum"],
        },
    },

    "Denmark": {
        "Capital Region": {
            "Copenhagen": ["Frederiksberg", "Gentofte", "Gladsaxe",
                           "Lyngby-Taarbæk", "Hvidovre", "Roskilde"],
        },
        "Central Jutland": {
            "Aarhus": ["Viborg", "Herning", "Silkeborg", "Horsens", "Skanderborg"],
        },
        "Southern Denmark": {
            "Odense": ["Vejle", "Esbjerg", "Kolding", "Fredericia", "Aabenraa"],
        },
        "North Jutland": {
            "Aalborg": ["Hjørring", "Frederikshavn", "Thisted", "Brønderslev"],
        },
    },

    "Finland": {
        "Uusimaa": {
            "Helsinki": ["Espoo", "Vantaa", "Tampere", "Kauniainen",
                         "Hyvinkää", "Järvenpää"],
        },
        "Pirkanmaa": {
            "Tampere": ["Nokia", "Ylöjärvi", "Kangasala", "Lempäälä"],
        },
        "Southwest Finland": {
            "Turku": ["Naantali", "Raisio", "Kaarina", "Lieto"],
        },
        "North Ostrobothnia": {
            "Oulu": ["Kempele", "Liminka", "Muhos", "Haukipudas"],
        },
    },

    "Poland": {
        "Masovian": {
            "Warsaw": ["Praga-Południe", "Ursynów", "Mokotów", "Wola",
                       "Radom", "Legionowo", "Pruszków"],
        },
        "Lesser Poland": {
            "Kraków": ["Wieliczka", "Tarnów", "Nowy Sącz", "Nowy Targ"],
        },
        "Lower Silesian": {
            "Wrocław": ["Wałbrzych", "Legnica", "Jelenia Góra", "Lubin"],
        },
        "Greater Poland": {
            "Poznań": ["Gniezno", "Kalisz", "Piła", "Ostrów Wielkopolski"],
        },
        "Pomeranian": {
            "Gdańsk": ["Gdynia", "Sopot", "Słupsk", "Tczew"],
        },
        "Silesian": {
            "Katowice": ["Gliwice", "Sosnowiec", "Bytom", "Zabrze",
                         "Ruda Śląska", "Tychy"],
        },
        "Łódź": {
            "Łódź": ["Piotrków Trybunalski", "Skierniewice", "Zgierz"],
        },
        "Lublin": {
            "Lublin": ["Chełm", "Zamość", "Biała Podlaska", "Puławy"],
        },
    },

    "Portugal": {
        "Lisbon": {
            "Lisbon": ["Amadora", "Sintra", "Loures", "Cascais",
                       "Almada", "Setubal", "Barreiro"],
        },
        "Porto": {
            "Porto": ["Gaia", "Matosinhos", "Braga", "Gondomar",
                      "Maia", "Guimarães"],
        },
        "Algarve": {
            "Faro": ["Loulé", "Portimão", "Silves", "Olhão", "Albufeira"],
        },
        "Central Portugal": {
            "Coimbra": ["Leiria", "Viseu", "Aveiro", "Guarda", "Castelo Branco"],
        },
    },

    "Czech Republic": {
        "Prague": {
            "Prague": ["Středočeský kraj", "Kladno", "Mladá Boleslav", "Příbram"],
        },
        "South Moravian": {
            "Brno": ["Zlín", "Olomouc", "Hodonín", "Znojmo"],
        },
        "Moravian-Silesian": {
            "Ostrava": ["Opava", "Karviná", "Frýdek-Místek", "Havířov"],
        },
        "Pilsen": {
            "Plzeň": ["Karlovy Vary", "Sokolov", "Cheb"],
        },
    },

    "Greece": {
        "Attica": {
            "Athens": ["Piraeus", "Peristeri", "Kallithea", "Nikaia",
                       "Glyfada", "Kifissia", "Marousi"],
        },
        "Central Macedonia": {
            "Thessaloniki": ["Kalamaria", "Pavlos Melas", "Ampelokipoi",
                             "Neapoli-Sykies", "Pylaia-Chortiatis"],
        },
        "Western Greece": {
            "Patras": ["Agrinio", "Pyrgos", "Aigio", "Messolonghi"],
        },
        "Crete": {
            "Heraklion": ["Rethymno", "Chania", "Agios Nikolaos", "Sitia"],
        },
    },

    "Ireland": {
        "Leinster": {
            "Dublin": ["Dún Laoghaire-Rathdown", "Fingal", "South Dublin",
                       "Wicklow", "Drogheda", "Dundalk"],
        },
        "Munster": {
            "Cork": ["Limerick", "Waterford", "Killarney", "Tralee", "Ennis"],
        },
        "Connacht": {
            "Galway": ["Castlebar", "Sligo", "Roscommon", "Ballina"],
        },
        "Ulster (ROI)": {
            "Donegal": ["Monaghan", "Cavan", "Letterkenny"],
        },
    },

    "Hungary": {
        "Central Hungary": {
            "Budapest": ["Debrecen", "Miskolc", "Nyíregyháza", "Pécs"],
        },
        "Northern Great Plain": {
            "Debrecen": ["Nyíregyháza", "Berettyóújfalu", "Hajdúböszörmény"],
        },
        "Southern Transdanubia": {
            "Pécs": ["Kaposvár", "Szekszárd", "Dombóvár"],
        },
        "Western Transdanubia": {
            "Győr": ["Sopron", "Szombathely", "Zalaegerszeg", "Nagykanizsa"],
        },
    },

    "Romania": {
        "Bucharest-Ilfov": {
            "Bucharest": ["Voluntari", "Popești-Leordeni", "Pantelimon",
                          "Chiajna", "Bragadiru"],
        },
        "Cluj": {
            "Cluj-Napoca": ["Turda", "Dej", "Câmpia Turzii", "Gherla"],
        },
        "Timiș": {
            "Timișoara": ["Lugoj", "Buziaș", "Sânnicolau Mare"],
        },
        "Iași": {
            "Iași": ["Pașcani", "Hârlău", "Târgu Frumos"],
        },
        "Constanța": {
            "Constanța": ["Mangalia", "Medgidia", "Năvodari", "Cernavodă"],
        },
    },

    "Ukraine": {
        "Kyiv": {
            "Kyiv": ["Brovary", "Boryspil", "Irpin", "Bucha", "Bila Tserkva"],
        },
        "Kharkiv": {
            "Kharkiv": ["Lozova", "Chuhuiv", "Merefa", "Izium"],
        },
        "Dnipropetrovsk": {
            "Dnipro": ["Kamianske", "Kryvyi Rih", "Nikopol", "Pavlohrad"],
        },
        "Lviv": {
            "Lviv": ["Drohobych", "Stryi", "Boryslav", "Truskavets"],
        },
        "Odessa": {
            "Odessa": ["Chornomorsk", "Yuzhne", "Bilhorod-Dnistrovskyi"],
        },
    },

    "Russia": {
        "Moscow Oblast": {
            "Moscow": ["Mytishchi", "Balashikha", "Khimki", "Lyubertsy",
                       "Podolsk", "Krasnogorsk"],
        },
        "Saint Petersburg": {
            "Saint Petersburg": ["Pushkin", "Peterhof", "Kronshtadt",
                                 "Kolpino", "Gatchina"],
        },
        "Novosibirsk Oblast": {
            "Novosibirsk": ["Berdsk", "Ob", "Iskitim", "Krasnoobsk"],
        },
        "Sverdlovsk Oblast": {
            "Yekaterinburg": ["Nizhny Tagil", "Kamensk-Uralsky", "Pervouralsk"],
        },
        "Tatarstan": {
            "Kazan": ["Naberezhnye Chelny", "Nizhnekamsk", "Almetyevsk"],
        },
        "Krasnodar Krai": {
            "Krasnodar": ["Sochi", "Novorossiysk", "Armavir", "Anapa"],
        },
    },

    # Smaller European countries
    "Albania": {
        "Tirana County": {
            "Tirana": ["Durrës", "Elbasan", "Vlorë", "Shkodër"],
        },
    },
    "Armenia": {
        "Yerevan": {
            "Yerevan": ["Gyumri", "Vanadzor", "Vagharshapat", "Abovyan"],
        },
    },
    "Azerbaijan": {
        "Baku": {
            "Baku": ["Sumqayıt", "Ganja", "Mingəçevir", "Nakhchivan"],
        },
    },
    "Belarus": {
        "Minsk Region": {
            "Minsk": ["Brest", "Grodno", "Gomel", "Mogilev", "Vitebsk"],
        },
    },
    "Bosnia and Herzegovina": {
        "Federation of BiH": {
            "Sarajevo": ["Banja Luka", "Tuzla", "Zenica", "Mostar"],
        },
    },
    "Bulgaria": {
        "Sofia": {
            "Sofia": ["Plovdiv", "Varna", "Burgas", "Stara Zagora"],
        },
    },
    "Croatia": {
        "Zagreb County": {
            "Zagreb": ["Split", "Rijeka", "Osijek", "Zadar", "Slavonski Brod"],
        },
    },
    "Cyprus": {
        "Nicosia": {
            "Nicosia": ["Limassol", "Larnaca", "Paphos", "Famagusta"],
        },
    },
    "Estonia": {
        "Harju County": {
            "Tallinn": ["Tartu", "Narva", "Pärnu", "Kohtla-Järve"],
        },
    },
    "Georgia": {
        "Tbilisi": {
            "Tbilisi": ["Kutaisi", "Rustavi", "Batumi", "Zugdidi", "Gori"],
        },
    },
    "Iceland": {
        "Capital Region": {
            "Reykjavik": ["Kópavogur", "Hafnarfjörður", "Akureyri", "Akranes"],
        },
    },
    "Latvia": {
        "Riga": {
            "Riga": ["Jūrmala", "Jēkabpils", "Daugavpils", "Ventspils"],
        },
    },
    "Lithuania": {
        "Vilnius County": {
            "Vilnius": ["Kaunas", "Klaipėda", "Šiauliai", "Panevėžys"],
        },
    },
    "Luxembourg": {
        "Luxembourg District": {
            "Luxembourg City": ["Esch-sur-Alzette", "Differdange", "Dudelange"],
        },
    },
    "Malta": {
        "Southern Harbour": {
            "Valletta": ["Birkirkara", "Qormi", "Sliema", "Mosta"],
        },
    },
    "Moldova": {
        "Chișinău": {
            "Chișinău": ["Tiraspol", "Bălți", "Bender", "Ungheni"],
        },
    },
    "Montenegro": {
        "Podgorica": {
            "Podgorica": ["Nikšić", "Bar", "Budva", "Kotor"],
        },
    },
    "North Macedonia": {
        "Skopje": {
            "Skopje": ["Bitola", "Kumanovo", "Tetovo", "Ohrid"],
        },
    },
    "Serbia": {
        "Belgrade": {
            "Belgrade": ["Novi Sad", "Niš", "Kragujevac", "Subotica",
                         "Zrenjanin", "Pančevo"],
        },
    },
    "Slovakia": {
        "Bratislava Region": {
            "Bratislava": ["Košice", "Prešov", "Nitra", "Žilina", "Banská Bystrica"],
        },
    },
    "Slovenia": {
        "Ljubljana": {
            "Ljubljana": ["Maribor", "Celje", "Kranj", "Koper", "Velenje"],
        },
    },

    # =========================================================================
    # MIDDLE EAST
    # =========================================================================

    "United Arab Emirates": {
        "Dubai": {
            "Dubai": ["Deira", "Bur Dubai", "Jumeirah", "Al Quoz",
                      "Dubai Silicon Oasis", "Jebel Ali"],
        },
        "Abu Dhabi": {
            "Abu Dhabi": ["Al Ain", "Ruwais", "Musaffah", "Khalifa City"],
        },
        "Sharjah": {
            "Sharjah": ["Ajman", "Umm Al Quwain", "Ras Al Khaimah", "Fujairah"],
        },
    },

    "Saudi Arabia": {
        "Riyadh Region": {
            "Riyadh": ["Kharj", "Diriyah", "Al Majmaah", "Zulfi"],
        },
        "Makkah Region": {
            "Jeddah": ["Mecca", "Taif", "Rabigh", "Al-Qunfudhah"],
        },
        "Eastern Province": {
            "Dammam": ["Al-Ahsa", "Al Khobar", "Dhahran", "Jubail", "Qatif"],
        },
        "Medina Region": {
            "Medina": ["Yanbu", "Al Ula", "Badr", "Mahd adh Dhahab"],
        },
        "Asir Region": {
            "Abha": ["Khamis Mushait", "Bisha", "Al-Namas", "Sarat Abidah"],
        },
    },

    "Qatar": {
        "Doha": {
            "Doha": ["Al Rayyan", "Al Wakrah", "Al Khor", "Umm Salal"],
        },
    },

    "Kuwait": {
        "Capital Governorate": {
            "Kuwait City": ["Hawalli", "Farwaniya", "Ahmadi", "Jahra"],
        },
    },

    "Bahrain": {
        "Capital Governorate": {
            "Manama": ["Muharraq", "Riffa", "Hamad Town", "Isa Town"],
        },
    },

    "Oman": {
        "Muscat Governorate": {
            "Muscat": ["Seeb", "Salalah", "Sohar", "Nizwa", "Sur"],
        },
    },

    "Israel": {
        "Tel Aviv District": {
            "Tel Aviv": ["Bnei Brak", "Ramat Gan", "Petah Tikva",
                         "Holon", "Bat Yam"],
        },
        "Jerusalem District": {
            "Jerusalem": ["Bethlehem", "Ramallah", "Beit Shemesh", "Ma'ale Adumim"],
        },
        "Haifa District": {
            "Haifa": ["Acre", "Nazareth", "Tiberias", "Karmiel"],
        },
        "Southern District": {
            "Beersheba": ["Ashkelon", "Ashdod", "Eilat", "Dimona"],
        },
    },

    "Jordan": {
        "Amman Governorate": {
            "Amman": ["Zarqa", "Irbid", "Aqaba", "Madaba", "Salt"],
        },
    },

    "Lebanon": {
        "Beirut": {
            "Beirut": ["Tripoli", "Sidon", "Tyre", "Jounieh", "Baalbek"],
        },
    },

    "Iraq": {
        "Baghdad Governorate": {
            "Baghdad": ["Mosul", "Basra", "Erbil", "Sulaymaniyah", "Najaf"],
        },
    },

    "Iran": {
        "Tehran Province": {
            "Tehran": ["Karaj", "Rey", "Shahr-e-Qods", "Varamin"],
        },
        "Isfahan Province": {
            "Isfahan": ["Kashan", "Najafabad", "Khomeyni Shahr"],
        },
        "Razavi Khorasan": {
            "Mashhad": ["Nishapur", "Sabzevar", "Gonabad"],
        },
        "Fars Province": {
            "Shiraz": ["Marvdasht", "Kavar", "Jahrom", "Neyriz"],
        },
        "East Azerbaijan": {
            "Tabriz": ["Urmia", "Ardabil", "Marand", "Bonab"],
        },
        "Khuzestan Province": {
            "Ahvaz": ["Dezful", "Abadan", "Khorramshahr", "Masjed Soleiman"],
        },
    },

    # =========================================================================
    # ASIA
    # =========================================================================

    "China": {
        "Beijing": {
            "Beijing": ["Tongzhou", "Changping", "Daxing", "Shunyi",
                        "Fangshan", "Mentougou"],
        },
        "Shanghai": {
            "Shanghai": ["Pudong", "Minhang", "Baoshan", "Jiading",
                         "Qingpu", "Songjiang", "Fengxian"],
        },
        "Guangdong": {
            "Guangzhou": ["Foshan", "Dongguan", "Zhongshan", "Huizhou",
                          "Zhuhai", "Jiangmen"],
            "Shenzhen": ["Nanshan", "Futian", "Luohu", "Longhua", "Baoan"],
        },
        "Jiangsu": {
            "Nanjing": ["Suzhou", "Wuxi", "Changzhou", "Nantong", "Xuzhou"],
        },
        "Zhejiang": {
            "Hangzhou": ["Ningbo", "Wenzhou", "Jiaxing", "Huzhou",
                         "Shaoxing", "Taizhou"],
        },
        "Shandong": {
            "Jinan": ["Qingdao", "Zibo", "Linyi", "Weifang",
                      "Yantai", "Jining"],
        },
        "Sichuan": {
            "Chengdu": ["Mianyang", "Deyang", "Yibin", "Luzhou",
                        "Nanchong", "Leshan"],
        },
        "Hubei": {
            "Wuhan": ["Yichang", "Xiangyang", "Jingzhou", "Huangshi"],
        },
        "Hunan": {
            "Changsha": ["Zhuzhou", "Xiangtan", "Hengyang", "Yueyang"],
        },
        "Shaanxi": {
            "Xi'an": ["Xianyang", "Baoji", "Weinan", "Hanzhong"],
        },
        "Liaoning": {
            "Shenyang": ["Dalian", "Anshan", "Fushun", "Benxi", "Dandong"],
        },
        "Heilongjiang": {
            "Harbin": ["Qiqihar", "Mudanjiang", "Jiamusi", "Daqing"],
        },
        "Henan": {
            "Zhengzhou": ["Luoyang", "Xinyang", "Nanyang", "Kaifeng"],
        },
        "Fujian": {
            "Fuzhou": ["Xiamen", "Quanzhou", "Putian", "Zhangzhou"],
        },
        "Yunnan": {
            "Kunming": ["Qujing", "Yuxi", "Dali", "Lijiang"],
        },
        "Xinjiang": {
            "Urumqi": ["Kashgar", "Korla", "Shihezi", "Aksu"],
        },
        "Inner Mongolia": {
            "Hohhot": ["Baotou", "Chifeng", "Ordos", "Tongliao"],
        },
        "Tianjin": {
            "Tianjin": ["Binhai New Area", "Dongli", "Jinnan", "Xiqing"],
        },
        "Chongqing": {
            "Chongqing": ["Yubei", "Jiulongpo", "Shapingba", "Nan'an"],
        },
        "Anhui": {
            "Hefei": ["Wuhu", "Bengbu", "Huainan", "Ma'anshan"],
        },
        "Jiangxi": {
            "Nanchang": ["Ganzhou", "Jingdezhen", "Jiujiang", "Xinyu"],
        },
        "Guizhou": {
            "Guiyang": ["Zunyi", "Anshun", "Kaili", "Tongren"],
        },
        "Shanxi": {
            "Taiyuan": ["Datong", "Yangquan", "Changzhi", "Jincheng"],
        },
    },

    "Japan": {
        "Tokyo": {
            "Tokyo": ["Yokohama", "Kawasaki", "Sagamihara", "Chiba",
                      "Funabashi", "Hachioji"],
        },
        "Osaka": {
            "Osaka": ["Sakai", "Higashiosaka", "Hirakata", "Toyonaka",
                      "Suita", "Ibaraki"],
        },
        "Kanagawa": {
            "Yokohama": ["Kawasaki", "Sagamihara", "Fujisawa",
                         "Yokosuka", "Kamakura"],
        },
        "Aichi": {
            "Nagoya": ["Toyota", "Okazaki", "Ichinomiya", "Kasugai",
                       "Toyohashi", "Nishio"],
        },
        "Fukuoka": {
            "Fukuoka": ["Kitakyushu", "Kurume", "Kasuga", "Onojo",
                        "Dazaifu"],
        },
        "Hokkaido": {
            "Sapporo": ["Hakodate", "Asahikawa", "Obihiro", "Kushiro",
                        "Otaru"],
        },
        "Hyogo": {
            "Kobe": ["Himeji", "Akashi", "Nishi-ku", "Itami",
                     "Amagasaki"],
        },
        "Kyoto": {
            "Kyoto": ["Uji", "Nagaokakyo", "Muko", "Kameoka"],
        },
        "Hiroshima": {
            "Hiroshima": ["Fukuyama", "Kure", "Higashihiroshima",
                          "Onomichi"],
        },
        "Miyagi": {
            "Sendai": ["Ishinomaki", "Osaki", "Natori", "Tome"],
        },
        "Okinawa": {
            "Naha": ["Okinawa City", "Urasoe", "Nago", "Ginowan"],
        },
    },

    "India": {
        "Maharashtra": {
            "Mumbai": ["Thane", "Navi Mumbai", "Kalyan", "Ulhasnagar",
                       "Vasai-Virar", "Aurangabad", "Nagpur"],
            "Pune": ["Pimpri-Chinchwad", "Nashik", "Solapur", "Kolhapur"],
        },
        "Karnataka": {
            "Bangalore": ["Mysore", "Hubli", "Mangalore", "Belgaum",
                          "Gulbarga", "Davanagere"],
        },
        "Tamil Nadu": {
            "Chennai": ["Coimbatore", "Madurai", "Tiruchirappalli",
                        "Salem", "Tirunelveli", "Erode"],
        },
        "Telangana": {
            "Hyderabad": ["Warangal", "Nizamabad", "Khammam",
                          "Karimnagar", "Secunderabad"],
        },
        "Delhi NCR": {
            "Delhi": ["Gurgaon", "Noida", "Faridabad", "Ghaziabad",
                      "Greater Noida"],
        },
        "Gujarat": {
            "Ahmedabad": ["Surat", "Vadodara", "Rajkot", "Bhavnagar",
                          "Jamnagar", "Gandhinagar"],
        },
        "Rajasthan": {
            "Jaipur": ["Jodhpur", "Kota", "Bikaner", "Ajmer",
                       "Udaipur", "Bhilwara"],
        },
        "West Bengal": {
            "Kolkata": ["Howrah", "Asansol", "Siliguri", "Durgapur",
                        "Bardhaman"],
        },
        "Uttar Pradesh": {
            "Lucknow": ["Kanpur", "Agra", "Varanasi", "Prayagraj",
                        "Meerut", "Ghaziabad"],
        },
        "Punjab": {
            "Chandigarh": ["Ludhiana", "Amritsar", "Jalandhar",
                           "Patiala", "Bathinda"],
        },
        "Kerala": {
            "Thiruvananthapuram": ["Kochi", "Kozhikode", "Thrissur",
                                   "Kollam", "Kannur"],
        },
        "Odisha": {
            "Bhubaneswar": ["Cuttack", "Rourkela", "Berhampur", "Sambalpur"],
        },
        "Madhya Pradesh": {
            "Bhopal": ["Indore", "Gwalior", "Jabalpur", "Ujjain", "Rewa"],
        },
        "Andhra Pradesh": {
            "Visakhapatnam": ["Vijayawada", "Tirupati", "Rajahmundry",
                              "Kakinada", "Nellore"],
        },
        "Haryana": {
            "Gurgaon": ["Faridabad", "Panipat", "Ambala", "Yamunanagar",
                        "Rohtak", "Hisar"],
        },
        "Jharkhand": {
            "Ranchi": ["Jamshedpur", "Dhanbad", "Bokaro", "Hazaribagh"],
        },
    },

    "South Korea": {
        "Seoul Capital Area": {
            "Seoul": ["Incheon", "Suwon", "Seongnam", "Bucheon",
                      "Goyang", "Ansan", "Hwaseong"],
        },
        "Gyeonggi": {
            "Suwon": ["Yongin", "Anyang", "Namyangju", "Pyeongtaek",
                      "Siheung"],
        },
        "Gyeongnam": {
            "Busan": ["Ulsan", "Changwon", "Gimhae", "Yangsan", "Jinju"],
        },
        "North Gyeongsang": {
            "Daegu": ["Pohang", "Gumi", "Gyeongju", "Andong"],
        },
        "South Chungcheong": {
            "Daejeon": ["Cheongju", "Sejong", "Cheonan", "Asan", "Gongju"],
        },
        "Jeollanam": {
            "Gwangju": ["Jeonju", "Yeosu", "Suncheon", "Mokpo"],
        },
        "Gangwon": {
            "Chuncheon": ["Wonju", "Gangneung", "Sokcho", "Donghae"],
        },
        "Jeju": {
            "Jeju City": ["Seogwipo", "Aewol", "Jocheon"],
        },
    },

    "Australia": {
        "New South Wales": {
            "Sydney": ["Parramatta", "Newcastle", "Wollongong", "Blacktown",
                       "Penrith", "Liverpool", "Campbelltown"],
            "Canberra": ["Queanbeyan", "Gungahlin", "Tuggeranong", "Belconnen"],
        },
        "Victoria": {
            "Melbourne": ["Geelong", "Ballarat", "Bendigo", "Frankston",
                          "Dandenong", "Ringwood", "Sunshine"],
        },
        "Queensland": {
            "Brisbane": ["Gold Coast", "Sunshine Coast", "Ipswich", "Toowoomba",
                         "Cairns", "Townsville", "Rockhampton"],
        },
        "Western Australia": {
            "Perth": ["Fremantle", "Rockingham", "Mandurah", "Joondalup",
                      "Bunbury", "Geraldton"],
        },
        "South Australia": {
            "Adelaide": ["Mount Barker", "Mount Gambier", "Port Augusta",
                         "Gawler", "Port Lincoln"],
        },
        "Tasmania": {
            "Hobart": ["Launceston", "Devonport", "Burnie", "Ulverstone"],
        },
    },

    "New Zealand": {
        "Auckland": {
            "Auckland": ["Manukau", "North Shore", "Waitakere", "Papakura",
                         "Rodney", "Franklin"],
        },
        "Wellington": {
            "Wellington": ["Lower Hutt", "Upper Hutt", "Porirua", "Kapiti"],
        },
        "Canterbury": {
            "Christchurch": ["Selwyn", "Waimakariri", "Ashburton", "Timaru"],
        },
        "Waikato": {
            "Hamilton": ["Whangarei", "Tauranga", "Rotorua", "Gisborne"],
        },
        "Otago": {
            "Dunedin": ["Queenstown", "Invercargill", "Alexandra", "Wanaka"],
        },
    },

    "Singapore": {
        "Central Region": {
            "Singapore": ["Jurong", "Woodlands", "Tampines", "Bedok",
                          "Pasir Ris", "Yishun"],
        },
    },

    "Malaysia": {
        "Kuala Lumpur": {
            "Kuala Lumpur": ["Petaling Jaya", "Shah Alam", "Subang Jaya",
                             "Klang", "Ampang Jaya", "Kajang"],
        },
        "Selangor": {
            "Shah Alam": ["Petaling Jaya", "Subang Jaya", "Klang",
                          "Rawang", "Sepang"],
        },
        "Penang": {
            "George Town": ["Butterworth", "Seberang Jaya", "Bayan Lepas",
                            "Nibong Tebal"],
        },
        "Johor": {
            "Johor Bahru": ["Batu Pahat", "Muar", "Kluang", "Segamat",
                            "Pontian"],
        },
        "Sabah": {
            "Kota Kinabalu": ["Sandakan", "Tawau", "Lahad Datu", "Keningau"],
        },
        "Sarawak": {
            "Kuching": ["Miri", "Sibu", "Bintulu", "Samarahan"],
        },
    },

    "Indonesia": {
        "DKI Jakarta": {
            "Jakarta": ["Bekasi", "Depok", "Tangerang", "South Tangerang",
                        "Bogor", "Cikarang"],
        },
        "East Java": {
            "Surabaya": ["Malang", "Sidoarjo", "Gresik", "Kediri",
                         "Jember", "Probolinggo"],
        },
        "West Java": {
            "Bandung": ["Cimahi", "Garut", "Sukabumi", "Tasikmalaya",
                        "Karawang", "Bogor"],
        },
        "North Sumatra": {
            "Medan": ["Deli Serdang", "Binjai", "Tebing Tinggi", "Pematangsiantar"],
        },
        "South Sulawesi": {
            "Makassar": ["Gowa", "Maros", "Sungguminasa", "Takalar"],
        },
        "Bali": {
            "Denpasar": ["Badung", "Gianyar", "Tabanan", "Singaraja"],
        },
        "Central Java": {
            "Semarang": ["Solo", "Yogyakarta", "Magelang", "Salatiga",
                         "Purwokerto"],
        },
        "South Kalimantan": {
            "Banjarmasin": ["Banjarbaru", "Martapura", "Pelaihari"],
        },
        "Papua": {
            "Jayapura": ["Sentani", "Nabire", "Sorong", "Manokwari"],
        },
    },

    "Thailand": {
        "Bangkok Metropolis": {
            "Bangkok": ["Nonthaburi", "Samut Prakan", "Pathum Thani",
                        "Samut Sakhon", "Nakhon Pathom"],
        },
        "Chiang Mai": {
            "Chiang Mai": ["Lamphun", "Lampang", "Mae Hong Son", "Chiang Rai"],
        },
        "Khon Kaen": {
            "Khon Kaen": ["Udon Thani", "Ubon Ratchathani", "Nakhon Ratchasima",
                          "Loei"],
        },
        "Eastern Seaboard": {
            "Pattaya": ["Chonburi", "Rayong", "Chachoengsao", "Laem Chabang"],
        },
        "Phuket": {
            "Phuket": ["Surat Thani", "Krabi", "Nakhon Si Thammarat",
                       "Phatthalung"],
        },
    },

    "Vietnam": {
        "Hanoi": {
            "Hanoi": ["Ha Dong", "Soc Son", "Dong Anh", "Gia Lam",
                      "Long Bien"],
        },
        "Ho Chi Minh City": {
            "Ho Chi Minh City": ["Thu Duc", "Binh Duong", "Dong Nai",
                                 "Long An", "Ba Ria-Vung Tau"],
        },
        "Da Nang": {
            "Da Nang": ["Hoi An", "Tam Ky", "Quang Ngai", "Hue"],
        },
        "Can Tho": {
            "Can Tho": ["Long Xuyen", "Rach Gia", "My Tho", "Vinh Long"],
        },
        "Hai Phong": {
            "Hai Phong": ["Nam Dinh", "Thai Binh", "Hung Yen", "Quang Ninh"],
        },
    },

    "Philippines": {
        "Metro Manila": {
            "Manila": ["Quezon City", "Caloocan", "Davao", "Cebu City",
                       "Makati", "Taguig", "Pasig"],
        },
        "Central Visayas": {
            "Cebu City": ["Mandaue", "Lapu-Lapu", "Talisay", "Danao"],
        },
        "Davao Region": {
            "Davao": ["Tagum", "Panabo", "Digos", "Mati"],
        },
        "Central Luzon": {
            "San Fernando": ["Angeles", "Olongapo", "Cabanatuan",
                             "Tarlac", "Malolos"],
        },
        "Calabarzon": {
            "Calamba": ["Antipolo", "Batangas City", "Lucena",
                        "Lipa", "San Pablo"],
        },
    },

    "Pakistan": {
        "Punjab": {
            "Lahore": ["Faisalabad", "Rawalpindi", "Gujranwala",
                       "Multan", "Sargodha", "Sialkot"],
        },
        "Sindh": {
            "Karachi": ["Hyderabad", "Sukkur", "Mirpur Khas", "Nawabshah"],
        },
        "Islamabad Capital": {
            "Islamabad": ["Rawalpindi", "Attock", "Chakwal", "Jhelum"],
        },
        "Khyber Pakhtunkhwa": {
            "Peshawar": ["Mardan", "Mingora", "Abbottabad", "Dera Ismail Khan"],
        },
        "Balochistan": {
            "Quetta": ["Turbat", "Khuzdar", "Hub", "Gwadar"],
        },
    },

    "Bangladesh": {
        "Dhaka Division": {
            "Dhaka": ["Gazipur", "Narayanganj", "Narsingdi",
                      "Munshiganj", "Manikganj"],
        },
        "Chittagong Division": {
            "Chittagong": ["Cox's Bazar", "Comilla", "Chandpur", "Brahmanbaria"],
        },
        "Rajshahi Division": {
            "Rajshahi": ["Bogra", "Naogaon", "Pabna", "Sirajgonj"],
        },
        "Khulna Division": {
            "Khulna": ["Jessore", "Bagerhat", "Satkhira", "Kushtia"],
        },
    },

    "Kazakhstan": {
        "Nur-Sultan": {
            "Nur-Sultan": ["Almaty", "Karagandy", "Shymkent", "Aktobe"],
        },
        "Almaty": {
            "Almaty": ["Taraz", "Pavlodar", "Ust-Kamenogorsk", "Semey"],
        },
    },

    "Uzbekistan": {
        "Tashkent": {
            "Tashkent": ["Namangan", "Samarkand", "Andijan", "Bukhara",
                         "Qashqadaryo"],
        },
    },

    "Mongolia": {
        "Ulaanbaatar": {
            "Ulaanbaatar": ["Darkhan", "Erdenet", "Choibalsan", "Mörön"],
        },
    },

    "Taiwan": {
        "New Taipei": {
            "New Taipei": ["Taipei", "Taoyuan", "Taichung", "Tainan",
                           "Kaohsiung", "Hsinchu", "Keelung"],
        },
    },

    "Sri Lanka": {
        "Western Province": {
            "Colombo": ["Dehiwala", "Sri Jayawardenepura Kotte", "Kelaniya",
                        "Kaduwela", "Maharagama"],
        },
        "Central Province": {
            "Kandy": ["Nuwara Eliya", "Matale", "Gampola"],
        },
        "Southern Province": {
            "Galle": ["Matara", "Hambantota", "Tangalle"],
        },
    },

    "Nepal": {
        "Bagmati Province": {
            "Kathmandu": ["Pokhara", "Lalitpur", "Bhaktapur", "Biratnagar"],
        },
    },

    "Cambodia": {
        "Phnom Penh": {
            "Phnom Penh": ["Siem Reap", "Sihanoukville", "Battambang",
                           "Kampong Cham"],
        },
    },

    "Myanmar": {
        "Yangon Region": {
            "Yangon": ["Naypyidaw", "Mandalay", "Mawlamyine", "Bago"],
        },
    },

    "Laos": {
        "Vientiane": {
            "Vientiane": ["Luang Prabang", "Savannakhet", "Pakse",
                          "Thakhek"],
        },
    },

    # =========================================================================
    # AFRICA
    # =========================================================================

    "South Africa": {
        "Gauteng": {
            "Johannesburg": ["Sandton", "Soweto", "Randburg", "Midrand",
                             "Centurion", "Roodepoort"],
            "Pretoria": ["Centurion", "Soshanguve", "Mamelodi", "Atteridgeville"],
        },
        "Western Cape": {
            "Cape Town": ["Bellville", "Mitchell's Plain", "Khayelitsha",
                          "Somerset West", "Stellenbosch", "Paarl"],
        },
        "KwaZulu-Natal": {
            "Durban": ["Pietermaritzburg", "Umlazi", "Pinetown",
                       "Newcastle", "Richards Bay"],
        },
        "Eastern Cape": {
            "East London": ["Port Elizabeth", "Mthatha", "Uitenhage",
                            "Bhisho"],
        },
        "Free State": {
            "Bloemfontein": ["Welkom", "Botshabelo", "Sasolburg", "Phuthaditjhaba"],
        },
        "Limpopo": {
            "Polokwane": ["Tzaneen", "Mokopane", "Phalaborwa", "Louis Trichardt"],
        },
    },

    "Egypt": {
        "Cairo Governorate": {
            "Cairo": ["Giza", "Shubra El Kheima", "Helwan", "New Cairo",
                      "6th of October City", "El Obour"],
        },
        "Alexandria Governorate": {
            "Alexandria": ["Kafr El Dawwar", "El Beheira", "Marsa Matruh",
                           "Borg El Arab"],
        },
        "Aswan Governorate": {
            "Aswan": ["Luxor", "Qena", "Sohag", "Asyut"],
        },
        "Suez Governorate": {
            "Suez": ["Ismailia", "Port Said", "Al Arish", "El Minya"],
        },
    },

    "Nigeria": {
        "Lagos": {
            "Lagos": ["Ikeja", "Alimosho", "Mushin", "Surulere",
                      "Oshodi-Isolo", "Kosofe"],
        },
        "Federal Capital Territory": {
            "Abuja": ["Kano", "Ibadan", "Kaduna", "Port Harcourt"],
        },
        "Rivers": {
            "Port Harcourt": ["Obio/Akpor", "Eleme", "Oyigbo", "Okrika"],
        },
        "Oyo": {
            "Ibadan": ["Ogbomoso", "Oyo", "Iseyin", "Eruwa"],
        },
        "Kano": {
            "Kano": ["Kaduna", "Zaria", "Katsina", "Gusau"],
        },
    },

    "Kenya": {
        "Nairobi County": {
            "Nairobi": ["Ruiru", "Kikuyu", "Thika", "Limuru",
                        "Kiambu", "Ngong"],
        },
        "Coast County": {
            "Mombasa": ["Kwale", "Kilifi", "Malindi", "Lamu"],
        },
        "Kisumu County": {
            "Kisumu": ["Kakamega", "Eldoret", "Kericho", "Kisii"],
        },
        "Nakuru County": {
            "Nakuru": ["Kericho", "Bomet", "Naivasha", "Gilgil"],
        },
    },

    "Ethiopia": {
        "Addis Ababa": {
            "Addis Ababa": ["Dire Dawa", "Mekelle", "Gondar",
                            "Hawassa", "Bahir Dar", "Adama"],
        },
        "Oromia": {
            "Adama": ["Jimma", "Shashamane", "Asella", "Nekemte"],
        },
    },

    "Tanzania": {
        "Dar es Salaam": {
            "Dar es Salaam": ["Arusha", "Mwanza", "Dodoma",
                              "Zanzibar City", "Tanga", "Moshi"],
        },
    },

    "Ghana": {
        "Greater Accra": {
            "Accra": ["Kumasi", "Sekondi-Takoradi", "Tamale",
                      "Tema", "Ashaiman"],
        },
        "Ashanti": {
            "Kumasi": ["Obuasi", "Ejisu", "Konongo", "Bekwai"],
        },
    },

    "Morocco": {
        "Casablanca-Settat": {
            "Casablanca": ["Mohammedia", "El Jadida", "Berrechid",
                           "Settat", "Ben Slimane"],
        },
        "Rabat-Salé-Kénitra": {
            "Rabat": ["Salé", "Kénitra", "Skhirat", "Temara"],
        },
        "Fès-Meknès": {
            "Fez": ["Meknès", "Sefrou", "Ifrane", "Taza"],
        },
        "Marrakech-Safi": {
            "Marrakech": ["Safi", "Essaouira", "Kelaat Sraghna", "Youssoufia"],
        },
        "Souss-Massa": {
            "Agadir": ["Inezgane", "Tiznit", "Taroudant", "Ouarzazate"],
        },
    },

    "Tunisia": {
        "Tunis Governorate": {
            "Tunis": ["Sfax", "Sousse", "Kairouan", "Bizerte", "Gabes"],
        },
        "Sfax Governorate": {
            "Sfax": ["Gabes", "Mahdia", "Gafsa", "Kasserine"],
        },
    },

    "Algeria": {
        "Algiers": {
            "Algiers": ["Oran", "Constantine", "Annaba", "Blida",
                        "Batna", "Tlemcen"],
        },
        "Oran": {
            "Oran": ["Mostaganem", "Ain Temouchent", "Relizane", "Tlemcen"],
        },
    },

    "Angola": {
        "Luanda Province": {
            "Luanda": ["Huambo", "Lobito", "Benguela", "Namibe",
                       "Malanje", "Lubango"],
        },
    },

    "Cameroon": {
        "Centre Region": {
            "Yaoundé": ["Douala", "Bamenda", "Bafoussam",
                        "Garoua", "Maroua"],
        },
        "Littoral Region": {
            "Douala": ["Edéa", "Nkongsamba", "Loum"],
        },
    },

    "Senegal": {
        "Dakar Region": {
            "Dakar": ["Touba", "Thiès", "Kaolack", "Saint-Louis",
                      "Ziguinchor", "Rufisque"],
        },
    },

    "Uganda": {
        "Central Region": {
            "Kampala": ["Entebbe", "Jinja", "Gulu", "Mbarara", "Lira"],
        },
    },

    "Zimbabwe": {
        "Harare Province": {
            "Harare": ["Bulawayo", "Chitungwiza", "Mutare",
                       "Gweru", "Kwekwe"],
        },
    },

    "Zambia": {
        "Lusaka Province": {
            "Lusaka": ["Kitwe", "Ndola", "Livingstone", "Kabwe", "Chipata"],
        },
    },

    "Rwanda": {
        "City of Kigali": {
            "Kigali": ["Gitarama", "Butare", "Gisenyi", "Ruhengeri"],
        },
    },

    "Mozambique": {
        "Maputo City": {
            "Maputo": ["Matola", "Beira", "Nampula", "Nacala",
                       "Quelimane", "Tete"],
        },
    },

    "Namibia": {
        "Khomas Region": {
            "Windhoek": ["Walvis Bay", "Swakopmund", "Rundu", "Oshakati"],
        },
    },

    "Botswana": {
        "South East District": {
            "Gaborone": ["Francistown", "Molepolole", "Mogoditshane",
                         "Maun", "Kanye"],
        },
    },

    "Sudan": {
        "Khartoum State": {
            "Khartoum": ["Omdurman", "Khartoum North", "Port Sudan",
                         "Kassala", "El Obeid"],
        },
    },

    "Libya": {
        "Tripoli District": {
            "Tripoli": ["Benghazi", "Misrata", "Bayda", "Zawiya", "Ajdabiya"],
        },
    },

    "Madagascar": {
        "Analamanga Region": {
            "Antananarivo": ["Toamasina", "Antsirabe", "Fianarantsoa",
                             "Mahajanga", "Toliara"],
        },
    },

    "Ivory Coast": {
        "Abidjan District": {
            "Abidjan": ["Bouaké", "Yamoussoukro", "Daloa", "San-Pédro",
                        "Korhogo"],
        },
    },

    "Mali": {
        "Bamako Capital District": {
            "Bamako": ["Sikasso", "Ségou", "Mopti", "Koutiala"],
        },
    },

    # =========================================================================
    # OCEANIA
    # =========================================================================

    "Papua New Guinea": {
        "National Capital District": {
            "Port Moresby": ["Lae", "Mount Hagen", "Madang", "Wewak",
                             "Goroka"],
        },
    },

    "Fiji": {
        "Central Division": {
            "Suva": ["Nadi", "Lautoka", "Labasa", "Ba"],
        },
    },

}

# ---------------------------------------------------------------------------
# All world countries (alphabetical) — kept for backward compatibility
# ---------------------------------------------------------------------------

ALL_COUNTRIES: list[str] = sorted(
    {c for countries in CONTINENT_COUNTRIES.values() for c in countries}
)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_continents() -> list[str]:
    """Return all continents in display order."""
    return list(CONTINENT_COUNTRIES.keys())


def get_countries_by_continent(continent: str) -> list[str]:
    """Return all countries for a given continent."""
    return CONTINENT_COUNTRIES.get(continent, [])


def get_countries() -> list[str]:
    """Return all world countries in alphabetical order."""
    return ALL_COUNTRIES


def get_regions(country: str) -> list[str]:
    """Return all regions/states for a given country."""
    return list(LOCATION_HIERARCHY.get(country, {}).keys())


def get_base_cities(country: str, region: str) -> list[str]:
    """Return all base cities for a given country and region."""
    return list(LOCATION_HIERARCHY.get(country, {}).get(region, {}).keys())


def get_sub_cities(country: str, region: str, city: str) -> list[str]:
    """
    Return the recommended metro sub-cities for a base city.
    Returns an empty list if the city has no sub-cities or is not in the hierarchy.
    """
    return list(
        LOCATION_HIERARCHY.get(country, {}).get(region, {}).get(city, [])
    )


def get_all_cities_flat(country: str, region: str) -> list[str]:
    """
    Return a flat list of all cities (base + sub) for a region.
    Useful for autocomplete or full-region search.
    """
    cities = []
    for base_city, sub_cities in LOCATION_HIERARCHY.get(country, {}).get(region, {}).items():
        cities.append(base_city)
        cities.extend(sub_cities)
    return cities


def is_known_location(country: str, region: str = "", city: str = "") -> bool:
    """
    Check if a location exists in the hierarchy.
    - country only: True if country in hierarchy
    - country + region: True if region in hierarchy
    - country + region + city: True if base city in hierarchy
    """
    if not region:
        return country in LOCATION_HIERARCHY
    if not city:
        return region in LOCATION_HIERARCHY.get(country, {})
    return city in LOCATION_HIERARCHY.get(country, {}).get(region, {})
