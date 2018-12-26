const { Observable } = require('rxjs');

const transform = {};


transform.combineNames = (data) => {
  const nd = {};
  nd.id = data.id * 1;
  nd.full_name = data["first_name"] + ' ' + data["last_name"];
  nd.email_address = data.email_address;
  nd.password = data.password;
  nd.phone = data.phone.replace(/[^0-9]/g, ''); 
  nd.street_address = data.street_address;
  nd.city = data.city;
  nd.postal_code = data.postal_code;
  nd.country = data.country;
  nd["__line"] = (data.id * 1) + 1;
  return nd;
};

module.exports = transform;