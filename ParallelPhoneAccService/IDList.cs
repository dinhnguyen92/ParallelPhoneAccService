using System;
using System.Collections.Generic;

namespace ParallelPhoneAccService
{
    public class IDList
    {
        public List<int> result { get; set; }
        public string token { get; set; }

        // Default construct to create an empty result list
        public IDList()
        {
            result = null;
            token = null;
        }

        // Convenient method to print out the list
        public override string ToString()
        {
            string res = "result: ";
            for (int i = 0; i < result.Count; i++)
            {
                res = res + result[i] + ", ";
            }

            if (!String.IsNullOrEmpty(token))
            {
                res = res + "token: " + token;
            }

            return res;
        }
    }
}
