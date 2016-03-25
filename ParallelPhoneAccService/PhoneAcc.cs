using System.Text.RegularExpressions;

namespace ParallelPhoneAccService
{
    public class PhoneAcc
    {
        public int id { get; set; }
        public string name { get; set; }
        public int age { get; set; }
        public string number { get; set; }

        // Default constructor to create an "empty" account
        public PhoneAcc()
        {
            id = -1;
            name = null;
            age = -1;
            number = null;
        }

        // Convenient method to print out the account
        public override string ToString()
        {
            string res = "ID: " + id + ", " + "name: " + name + ", " + "age: " + age + ", " + "number: " + number;
            return res;
        }

        // Test if the accont has a valid US phone number
        public bool hasValidNumber()
        {
            // The regex pattern for checking phone number was obtained from:
            // http://stackoverflow.com/questions/18091324/regex-to-match-all-us-phone-number-formats
            string pattern = @"\(?\d{3}\)?-? *\d{3}-? *-?\d{4}";

            Regex regex = new Regex(pattern, RegexOptions.Compiled);

            return regex.IsMatch(number);
        }
    }
}