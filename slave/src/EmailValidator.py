from validate_email import validate_email_or_fail
import time

class EmailValidator:

    def validate(self, email_address, trial=0):
        try:
            start = time.time()
            if validate_email_or_fail(
                email_address=email_address,
                check_format=True,
                check_blacklist=True,
                check_dns=True,
                dns_timeout=5,
                check_smtp=True,
                smtp_timeout=5,
                smtp_helo_host='localhost',
                smtp_from_address='noreply@example.com',
                smtp_skip_tls=False,
                smtp_tls_context=None
            ):
                end = time.time()
                print(email_address, end - start)
                return email_address, True, "Valid email"
            else:
                end = time.time()
                print(email_address, end - start)
                return email_address, False, "Invalid email address"
        except Exception as e:
            if any(x in str(e) for x in [
                'No nameserver found for domain',
                'Temporary error in email address verification',
                'Domain lookup timed out',
            ]):
                if trial <= 2:
                    return self.validate(email_address, trial=trial+1)
            return email_address, False, str(e)
    