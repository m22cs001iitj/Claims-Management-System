from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_restx import Api, Resource, fields
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Optional
import re
import psycopg2
import jwt
import datetime 
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = 'your_secret_key_here'

api = Api(app, version='1.0', title='Claims Management API', description='API for managing claims')

class ClaimStatus(Enum):
    SUBMITTED = "Submitted"
    UNDER_REVIEW = "Under Review"
    APPROVED = "Approved"
    REJECTED = "Rejected"
    CLOSED = "Closed"

@dataclass
class Policyholder:
    id: str
    name: str
    contact_number: str
    email: str
    date_of_birth: datetime

@dataclass
class Policy:
    id: str
    policyholder_id: str
    type: str
    start_date: datetime
    end_date: datetime
    coverage_amount: float
    premium: float

@dataclass
class Claim:
    id: str
    policy_id: str
    date_of_incident: datetime
    description: str
    amount: float
    status: ClaimStatus = ClaimStatus.SUBMITTED
    date_submitted: datetime = field(default_factory=datetime.datetime.now)

class ValidationError(Exception):
    pass

class BusinessRuleViolation(Exception):
    pass

class DatabaseError(Exception):
    pass

@contextmanager
def get_db_connection():
    DATABASE_URL = os.environ['DATABASE_URL']
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()

class ClaimsManagementSystem:
    def _execute_transaction(self, func, *args, **kwargs):
        with get_db_connection() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    result = func(cur, *args, **kwargs)
                conn.commit()
                return result
            except psycopg2.Error as e:
                conn.rollback()
                raise DatabaseError(str(e))

    def init_db(self):
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS policyholders (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        contact_number VARCHAR(20) NOT NULL,
                        email VARCHAR(100) NOT NULL UNIQUE,
                        date_of_birth DATE NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS policies (
                        id SERIAL PRIMARY KEY,
                        policyholder_id INTEGER NOT NULL,
                        type VARCHAR(50) NOT NULL,
                        start_date DATE NOT NULL,
                        end_date DATE NOT NULL,
                        coverage_amount DECIMAL(10, 2) NOT NULL,
                        premium DECIMAL(10, 2) NOT NULL,
                        FOREIGN KEY (policyholder_id) REFERENCES policyholders(id) ON DELETE CASCADE 
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS claims (
                        id SERIAL PRIMARY KEY,
                        policy_id INTEGER NOT NULL,
                        date_of_incident DATE NOT NULL,
                        description TEXT NOT NULL,
                        amount DECIMAL(10, 2) NOT NULL,
                        status VARCHAR(20) NOT NULL,
                        date_submitted DATE NOT NULL,
                        FOREIGN KEY (policy_id) REFERENCES policies(id) ON DELETE CASCADE
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS login_users (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(100) NOT NULL,
                        password VARCHAR(100) NOT NULL
                    )
                """)
            conn.commit()

    def authenticate_user(self, username, password):
        def query(cur):
            cur.execute("SELECT * FROM login_users WHERE username = %s AND password = %s", (username, password))
            return cur.fetchone()
        return self._execute_transaction(query)

    # Policyholder CRUD operations
    def create_policyholder(self, policyholder: Policyholder) -> None:
        def _create(cur):
            self._validate_policyholder(policyholder)
            cur.execute("""
                INSERT INTO policyholders (id, name, contact_number, email, date_of_birth)
                VALUES (%(id)s, %(name)s, %(contact_number)s, %(email)s, %(date_of_birth)s)
            """, policyholder.__dict__)
        self._execute_transaction(_create)

    def get_policyholder(self, policyholder_id: str) -> Optional[Policyholder]:
        def _get(cur):
            cur.execute("SELECT * FROM policyholders WHERE id = %(id)s", {'id': policyholder_id})
            result = cur.fetchone()
            if result:
                return Policyholder(**result)
            return None
        return self._execute_transaction(_get)
    
    def getAll_policyholder(self) -> Optional[Policyholder]:
        def _get(cur):
            cur.execute("SELECT * FROM policyholders")
            result = cur.fetchall()
            if result:
                return result
            return None
        return self._execute_transaction(_get)

    def update_policyholder(self, policyholder_id: str, name: Optional[str] = None, 
                            contact_number: Optional[str] = None, email: Optional[str] = None,
                            date_of_birth: Optional[datetime.datetime] = None) -> None:
        def _update(cur):
            policyholder = self.get_policyholder(policyholder_id)
            if not policyholder:
                raise ValidationError(f"Policyholder with ID {policyholder_id} does not exist")
            
            update_fields = []
            values = {'id': policyholder_id}
            if name:
                update_fields.append("name = %(name)s")
                values['name'] = name
            if contact_number:
                self._validate_phone_number(contact_number)
                update_fields.append("contact_number = %(contact_number)s")
                values['contact_number'] = contact_number
            if email:
                self._validate_email(email)
                update_fields.append("email = %(email)s")
                values['email'] = email
            if date_of_birth:
                self._validate_date_of_birth(date_of_birth)
                update_fields.append("date_of_birth = %(date_of_birth)s")
                values['date_of_birth'] = date_of_birth
            
            if update_fields:
                cur.execute(f"""
                    UPDATE policyholders
                    SET {', '.join(update_fields)}
                    WHERE id = %(id)s
                """, values)
        self._execute_transaction(_update)

    def delete_policyholder(self, policyholder_id: str) -> None:
        def _delete(cur):
            cur.execute("DELETE FROM policyholders WHERE id = %s", (policyholder_id,))
            if cur.rowcount == 0:
                raise ValidationError(f"Policyholder with ID {policyholder_id} does not exist")
        self._execute_transaction(_delete)

    # Policy CRUD operations
    def create_policy(self, policy: Policy) -> None:
        def _create(cur):
            self._validate_policy(cur, policy)
            cur.execute("""
                INSERT INTO policies (id, policyholder_id, type, start_date, end_date, coverage_amount, premium)
                VALUES (%(id)s, %(policyholder_id)s, %(type)s, %(start_date)s, %(end_date)s, %(coverage_amount)s, %(premium)s)
            """, policy.__dict__)
        self._execute_transaction(_create)

    def get_policy(self, policy_id: str) -> Optional[Policy]:
        def _get(cur):
            cur.execute("SELECT * FROM policies WHERE id = %(id)s", {'id': policy_id})
            result = cur.fetchone()
            if result:
                return Policy(**result)
            return None
        return self._execute_transaction(_get)
    
    def getAll_policy(self) -> Optional[Policy]:
        def _get(cur):
            cur.execute("SELECT * FROM policies")
            result = cur.fetchall()
            if result:
                return result
            return None
        return self._execute_transaction(_get)

    def update_policy(self, policy_id: str, type: Optional[str] = None, 
                      start_date: Optional[datetime.datetime] = None, end_date: Optional[datetime.datetime] = None, 
                      coverage_amount: Optional[float] = None, premium: Optional[float] = None) -> None:
        def _update(cur):
            policy = self.get_policy(policy_id)
            if not policy:
                raise ValidationError(f"Policy with ID {policy_id} does not exist")
            
            update_fields = []
            values = {'id': policy_id}
            if type:
                update_fields.append("type = %(type)s")
                values['type'] = type
            if start_date:
                update_fields.append("start_date = %(start_date)s")
                values['start_date'] = start_date
            if end_date:
                update_fields.append("end_date = %(end_date)s")
                values['end_date'] = end_date
            if coverage_amount is not None:
                update_fields.append("coverage_amount = %(coverage_amount)s")
                values['coverage_amount'] = coverage_amount
            if premium is not None:
                update_fields.append("premium = %(premium)s")
                values['premium'] = premium
            
            if update_fields:
                cur.execute(f"""
                    UPDATE policies
                    SET {', '.join(update_fields)}
                    WHERE id = %(id)s
                """, values)
            
            updated_policy = self.get_policy(policy_id)
            self._validate_policy(cur, updated_policy)
        self._execute_transaction(_update)

    def delete_policy(self, policy_id: str) -> None:
        def _delete(cur):
            cur.execute("DELETE FROM policies WHERE id = %s", (policy_id,))
            if cur.rowcount == 0:
                raise ValidationError(f"Policy with ID {policy_id} does not exist")
        self._execute_transaction(_delete)

    # Claim CRUD operations
    def create_claim(self, claim: Claim) -> None:
        def _create(cur):
            self._validate_claim(cur, claim)
            claim_dict = claim.__dict__.copy()
            claim_dict['status'] = claim.status.value  # Convert Enum to string
            cur.execute("""
                INSERT INTO claims (id, policy_id, date_of_incident, description, amount, status, date_submitted)
                VALUES (%(id)s, %(policy_id)s, %(date_of_incident)s, %(description)s, %(amount)s, %(status)s, %(date_submitted)s)
            """, claim_dict)
        self._execute_transaction(_create)

    def get_claim(self, claim_id: str) -> Optional[Claim]:
        def _get(cur):
            cur.execute("SELECT * FROM claims WHERE id = %(id)s", {'id': claim_id})
            result = cur.fetchone()
            if result:
                result['status'] = ClaimStatus(result['status'])
                return Claim(**result)
            return None
        return self._execute_transaction(_get)
    
    def getAll_claim(self) -> Optional[Claim]:
        def _get(cur):
            cur.execute("SELECT * FROM claims")
            result = cur.fetchall()
            if result:
                return result
            return None
        return self._execute_transaction(_get)

    def update_claim(self, claim_id: str, description: Optional[str] = None, 
                     amount: Optional[float] = None, status: Optional[ClaimStatus] = None) -> None:
        def _update(cur):
            claim = self.get_claim(claim_id)
            if not claim:
                raise ValidationError(f"Claim with ID {claim_id} does not exist")
            
            update_fields = []
            values = {'id': claim_id}
            if description:
                update_fields.append("description = %(description)s")
                values['description'] = description
            if amount is not None:
                update_fields.append("amount = %(amount)s")
                values['amount'] = amount
            if status:
                update_fields.append("status = %(status)s")
                values['status'] = status.value  # Convert Enum to string
            
            if update_fields:
                cur.execute(f"""
                    UPDATE claims
                    SET {', '.join(update_fields)}
                    WHERE id = %(id)s
                """, values)
            
            updated_claim = self.get_claim(claim_id)
            self._validate_claim(cur, updated_claim)
        self._execute_transaction(_update)

    def delete_claim(self, claim_id: str) -> None:
        def _delete(cur):
            cur.execute("DELETE FROM claims WHERE id = %s", (claim_id,))
            if cur.rowcount == 0:
                raise ValidationError(f"Claim with ID {claim_id} does not exist")
        self._execute_transaction(_delete)

    # Validation methods
    def _validate_policyholder(self, policyholder: Policyholder) -> None:
        self._validate_email(policyholder.email)
        self._validate_phone_number(policyholder.contact_number)
        self._validate_date_of_birth(policyholder.date_of_birth)

    def _validate_policy(self, cur, policy: Policy) -> None:
        cur.execute("SELECT * FROM policyholders WHERE id = %(id)s", {'id': policy.policyholder_id})
        policyholder = cur.fetchone()
        if not policyholder:
            raise ValidationError(f"Policyholder with ID {policy.policyholder_id} does not exist")
        if policy.start_date >= policy.end_date:
            raise ValidationError("Policy start date must be before end date")
        if policy.coverage_amount <= 0:
            raise ValidationError("Coverage amount must be positive")
        if policy.premium <= 0:
            raise ValidationError("Premium must be positive")
        policyholder_dob = policyholder['date_of_birth']
        if isinstance(policyholder_dob, datetime.datetime):
            policyholder_dob = policyholder_dob.date()
        if isinstance(policy.start_date, datetime.datetime):
            policy.start_date = policy.start_date.date()
        if (policy.start_date - policyholder_dob).days < 18 * 365:
            raise BusinessRuleViolation("Policyholder must be at least 18 years old at policy start date")

    def _validate_claim(self, cur, claim: Claim) -> None:
        cur.execute("SELECT * FROM policies WHERE id = %(id)s", {'id': claim.policy_id})
        policy = cur.fetchone()
        if not policy:
            raise ValidationError(f"Policy with ID {claim.policy_id} does not exist")
        policy_sd = policy['start_date']
        policy_ed = policy['end_date']
        policy_ca = policy['coverage_amount']
        if isinstance(policy_sd, datetime.datetime):
            policy_sd = policy_sd.date()
        if isinstance(policy_ed, datetime.datetime):
            policy_ed = policy_ed.date()
        if isinstance(claim.date_of_incident, datetime.datetime):
            claim.date_of_incident = claim.date_of_incident.date()
        if isinstance(claim.date_submitted, datetime.datetime):
            claim.date_submitted = claim.date_submitted.date()
        if claim.date_of_incident < policy_sd or claim.date_of_incident > policy_ed:
            raise ValidationError("Claim date must be within policy period")
        if claim.amount <= 0 or claim.amount > policy_ca:
            raise ValidationError(f"Claim amount must be positive and not exceed policy coverage of {policy_ca}")
        if claim.date_submitted < claim.date_of_incident:
            raise ValidationError("Claim submission date cannot be earlier than the incident date")
        if (claim.date_submitted - claim.date_of_incident).days > 30:
            raise BusinessRuleViolation("Claims must be submitted within 30 days of the incident")

    def _validate_email(self, email: str) -> None:
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            raise ValidationError("Invalid email format")

    def _validate_phone_number(self, phone: str) -> None:
        if not re.match(r"^\+?1?\d{9,15}$", phone):
            raise ValidationError("Invalid phone number format")

    def _validate_date_of_birth(self, date_of_birth: datetime.datetime) -> None:
        if date_of_birth > datetime.datetime.now():
            raise ValidationError("Date of birth cannot be in the future")
        if (datetime.datetime.now() - date_of_birth).days < 18 * 365:
            raise BusinessRuleViolation("Policyholder must be at least 18 years old")

cms = ClaimsManagementSystem()
cms.init_db()

# API routes

login_model = api.model('Login', {
    'username': fields.String(required=True, description='The username'),
    'password': fields.String(required=True, description='The password')
})

@api.route('/login')
class Login(Resource):
    @api.expect(login_model)
    @api.response(200, 'Success')
    @api.response(401, 'Invalid credentials')
    def post(self):
        auth_data = request.json
        user = cms.authenticate_user(auth_data['username'], auth_data['password'])
        if user:
            token = jwt.encode({'user': user['username'], 'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)}, 
                               app.config['SECRET_KEY'])
            return {'token': token}, 200
        return {'message': 'Invalid credentials'}, 401

# Helper function to parse dates
def parse_date(date_string):
    return datetime.datetime.strptime(date_string, "%Y-%m-%d")

policyholder_model = api.model('Policyholder', {
    'id': fields.String(required=True, description='Policyholder ID'),
    'name': fields.String(required=True, description='Policyholder name'),
    'contact_number': fields.String(required=True, description='Contact number'),
    'email': fields.String(required=True, description='Email address'),
    'date_of_birth': fields.Date(required=True, description='Date of birth')
})

@api.route('/policyholders')
class PolicyholderResource(Resource):
    @api.expect(policyholder_model)
    @api.response(201, 'Policyholder created successfully')
    @api.response(400, 'Validation error')
    def post(self):
        data = request.json
        try:
            policyholder = Policyholder(
                id=data['id'],
                name=data['name'],
                contact_number=data['contact_number'],
                email=data['email'],
                date_of_birth=parse_date(data['date_of_birth'])
            )
            cms.create_policyholder(policyholder)
            return {"message": "Policyholder created successfully"}, 201
        except (ValidationError, BusinessRuleViolation) as e:
            return {"error": str(e)}, 400

    @api.response(200, 'Success')
    @api.response(404, 'No policyholders found')
    def get(self):
        policyholders = cms.getAll_policyholder()
        if policyholders:
            return jsonify(policyholders)
        return {"error": "No policyholders found"}, 404

@api.route('/policyholders/<string:policyholder_id>')
class PolicyholderIdResource(Resource):
    @api.response(200, 'Success')
    @api.response(404, 'Policyholder not found')
    def get(self, policyholder_id):
        policyholder = cms.get_policyholder(policyholder_id)
        if policyholder:
            return jsonify({
                "id": policyholder.id,
                "name": policyholder.name,
                "contact_number": policyholder.contact_number,
                "email": policyholder.email,
                "date_of_birth": policyholder.date_of_birth.strftime("%Y-%m-%d")
            })
        return {"error": "Policyholder not found"}, 404

    @api.expect(policyholder_model)
    @api.response(200, 'Policyholder updated successfully')
    @api.response(400, 'Validation error')
    @api.response(404, 'Policyholder not found')
    def put(self, policyholder_id):
        data = request.json
        try:
            cms.update_policyholder(
                policyholder_id,
                name=data.get('name'),
                contact_number=data.get('contact_number'),
                email=data.get('email'),
                date_of_birth=parse_date(data['date_of_birth']) if 'date_of_birth' in data else None
            )
            return {"message": "Policyholder updated successfully"}
        except ValidationError as e:
            return {"error": str(e)}, 400
        except BusinessRuleViolation as e:
            return {"error": str(e)}, 400

    @api.response(200, 'Policyholder deleted successfully')
    @api.response(400, 'Validation error')
    def delete(self, policyholder_id):
        try:
            cms.delete_policyholder(policyholder_id)
            return {"message": "Policyholder deleted successfully"}
        except ValidationError as e:
            return {"error": str(e)}, 400

# Similar API routes can be added for Policy and Claim resources

@app.errorhandler(DatabaseError)
def handle_database_error(error):
    return jsonify({"error": str(error)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)
