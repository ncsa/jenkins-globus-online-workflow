#!/usr/bin/env python3
"""
Test mule for jenkins to monitor GO

Workflow:  jyc:~arnoldg/scratch/jenkins-mule/ -> bw -> nearline
           mac:~/.../status_file -> bw
           bwjenkins will check up on the status_file timestamp and performance

Based on tutorial and documentation at:
   http://globus.github.io/globus-sdk-python/index.html
-Galen Arnold, 2018, NCSA
"""
import time
import datetime
import os
import json
import webbrowser
import globus_sdk


# some globals
CLIENT_ID = '231634e4-37cc-4a06-96ce-12a262a62da7'
DEBUG = 0
TIMEOUT = 60
MB = 1048576
"""
Globus Online deadline means the job will be canceled when it has run this
duration no matter the state.  MB/s will still show in the status.  It's up
to the end user to decide how much of a failure that works out to be.
We'll let bwjenkins see the lower transfer performance and plot it.  DEADLINE
here is keeping us from overloading the endpoint(s) and it also tidies up
the polling requests for the jenkins_test_request file from the client side
python script.
"""
DEADLINE = 5
# To keep things simple, this is the same test space on jyc, bw, and nearline
MYSCRATCH = "/~/scratch/jenkins-mule"
TOKEN_FILE = 'refresh-tokens.json'
REDIRECT_URI = 'https://auth.globus.org/v2/web/auth-code'
SCOPES = ('openid email profile '
          'urn:globus:auth:scope:transfer.api.globus.org:all')
# endpoints determined by globus cli: globus endpoint search ncsa#jyc
#  or from globus.org -> "Manage Endpoints" -> endpoint detail, UUID
EP_BW = "d59900ef-6d04-11e5-ba46-22000b92c6ec"
EP_JYC = "d0ccdc02-6d04-11e5-ba46-22000b92c6ec"
EP_NEARLINE = "d599008e-6d04-11e5-ba46-22000b92c6ec"
EP_OFFICEMAC = "ce71c6f2-6d04-11e5-ba46-22000b92c6ec"
EP_H2OLOGIN1 = "7169968a-3288-11e8-b90d-0ac6873fc732"
UUID_FILE = open("uuid.txt", "r")
EP_OFFICEMAC = UUID_FILE.read()

GET_INPUT = getattr(__builtins__, 'raw_input', input)


def is_remote_session():
    """ Test if this is a remote ssh session """
    return os.environ.get('SSH_TTY', os.environ.get('SSH_CONNECTION'))


def load_tokens_from_file(filepath):
    """Load a set of saved tokens."""
    with open(filepath, 'r') as tokenfile:
        tokens = json.load(tokenfile)

    return tokens


def save_tokens_to_file(filepath, tokens):
    """Save a set of tokens for later use."""
    with open(filepath, 'w') as tokenfile:
        json.dump(tokens, tokenfile)


def update_tokens_file_on_refresh(token_response):
    """
    Callback function passed into the RefreshTokenAuthorizer.
    Will be invoked any time a new access token is fetched.
    """
    save_tokens_to_file(TOKEN_FILE, token_response.by_resource_server)


def do_native_app_authentication(client_id, redirect_uri,
                                 requested_scopes=None):
    """
    Does a Native App authentication flow and returns a
    dict of tokens keyed by service name.
    """
    client = globus_sdk.NativeAppAuthClient(client_id=client_id)
    # pass refresh_tokens=True to request refresh tokens
    client.oauth2_start_flow(requested_scopes=requested_scopes,
                             redirect_uri=redirect_uri,
                             refresh_tokens=True)

    url = client.oauth2_get_authorize_url()

    print('Native App Authorization URL: \n{}'.format(url))

    if not is_remote_session():
        webbrowser.open(url, new=1)

    auth_code = GET_INPUT('Enter the auth code: ').strip()

    token_response = client.oauth2_exchange_code_for_tokens(auth_code)

    # return a set of tokens, organized by resource server name
    return token_response.by_resource_server


def my_task_wait(tclient, result):
    """
    block on tclient task so that there's no overlap with the next transfer
    """
    while not tclient.task_wait(result["task_id"], timeout=TIMEOUT):
        print("Waiting on {0} to complete"
              .format(result["task_id"]))
# end def my_task_wait()


def my_delete(tclient, endpoint, mylabel, path, isrecursive):
    """
    DeleteData call wrapper
    """
    now = datetime.datetime.utcnow()
    mydeadline = now + datetime.timedelta(minutes=DEADLINE)
    ddata = globus_sdk.DeleteData(tclient, endpoint, label=mylabel,
                                  recursive=isrecursive,
                                  deadline=str(mydeadline))
    ddata.add_item(path)
    delete_result = tclient.submit_delete(ddata)
    print("ddata task_id = ", delete_result["task_id"])
    my_task_wait(tclient, delete_result)
# end def my_delete()


def my_transfer(tclient, srcpoint, destpoint, mylabel, srcpath,
                destpath, isrecursive):
    """
    TransferData call wrapper
    """
    now = datetime.datetime.utcnow()
    mydeadline = now + datetime.timedelta(minutes=DEADLINE)
    tdata = globus_sdk.TransferData(tclient, srcpoint, destpoint,
                                    label=mylabel,
                                    deadline=str(mydeadline),
                                    sync_level="checksum")
    tdata.add_item(srcpath, destpath, recursive=isrecursive)
    transfer_result = tclient.submit_transfer(tdata)
    print("tdata task_id = ", transfer_result["task_id"])

    if DEBUG == 1:
        print(tdata)
    my_task_wait(tclient, transfer_result)
    for event in tclient.task_event_list(transfer_result["task_id"]):
        print("Event on Task({}) at {}:\n{}".format(
            transfer_result["task_id"], event["time"], event["description"]))
# may want to log these also to a file
        error_file = open('error_file', 'a')
        if (event["is_error"]) and (event["description"] != "file not found"):
            print(" is_error:{}".format(
                event["details"]))
            error_file.write("%s: %s\n" % (event["time"], event["details"]))
            error_file.close()
# end def my_transfer()


def my_task_list(tclient):
    """
    The workflow happens here.  jyc->bw->nearline
    """
    # cleanup jenkins_mule/ on bw
    mylabel = " jenkins_cleanup_bw"
    print("WORKFLOW: ", mylabel)
    my_delete(tclient, EP_BW, mylabel, MYSCRATCH, True)
    # submit transfer jyc -> bw
    mylabel = "jenkins_jyc2bw"
    print("WORKFLOW: ", mylabel)
    my_transfer(tclient, EP_JYC, EP_BW, mylabel,
                MYSCRATCH,
                MYSCRATCH, True)
    mylabel = "jenkins_jyc2nearline"
    print("WORKFLOW ", mylabel)
    my_transfer(tclient, EP_JYC, EP_NEARLINE, mylabel,
                MYSCRATCH,
                MYSCRATCH, True)
    # cleanup jenkins_mule/ on nearline
    mylabel = "jenkins_cleanup_nearline"
    print("WORKFLOW: ", mylabel)
    my_delete(tclient, EP_NEARLINE, mylabel, MYSCRATCH, True)
    # submit transfer bw -> nearline
    mylabel = "jenkins_bw2nearline"
    print("WORKFLOW: ", mylabel)
    my_transfer(tclient, EP_BW, EP_NEARLINE, mylabel,
                MYSCRATCH,
                MYSCRATCH, True)
    # cleanup jenkins_mule/ on nearline
    mylabel = "jenkins_cleanup_nearline"
    print("WORKFLOW: ", mylabel)
    my_delete(tclient, EP_NEARLINE, mylabel, MYSCRATCH, True)

    # show some info about the transfers
    for transfer in tclient.task_list(num_results=7):
        if DEBUG == 1:
            print(transfer)
        print("task_id= ( %s ) %s -> %s"
              % (transfer['task_id'],
                 transfer['source_endpoint_display_name'],
                 transfer['destination_endpoint_display_name']))
        print("request_time %s" % transfer['request_time'])
        print("completed    %s" % transfer['completion_time'])
        print("type=%s Mbytes=%.1f Mbytes/s=%.1f files=%d"
              % (transfer['type'],
                 transfer['bytes_transferred']/MB,
                 transfer['effective_bytes_per_second']/MB,
                 transfer['files_transferred']))
    # write the info out to status_file
    status_file = open('status_file', 'a')
    for transfer in tclient.task_list(num_results=6):
        status_file.write("task_id= ( %s ) %s -> %s\n"
                          % (transfer['task_id'],
                             transfer['source_endpoint_display_name'],
                             transfer['destination_endpoint_display_name']))
        status_file.write("request_time %s\n" % transfer['request_time'])
        status_file.write("completed    %s\n" % transfer['completion_time'])
        status_file.write("type=%s Mbytes=%.1f Mbytes/s=%.1f files=%d\n"
                          % (transfer['type'],
                             transfer['bytes_transferred']/MB,
                             transfer['effective_bytes_per_second']/MB,
                             transfer['files_transferred']))
    status_file.close()
    os.system("date >> status_file")

    # send the status_file and error_file up to bw, for jenkins to monitor
    mylabel = "jenkins_status2bw"
    print("WORKFLOW: ", mylabel)
    my_transfer(tclient, EP_OFFICEMAC, EP_BW, mylabel,
                "/~/globus-cli/jenkins-globus-online-workflow/status_file",
                "/~/jenkins-mule/status_file", False)
    my_transfer(tclient, EP_OFFICEMAC, EP_BW, mylabel,
                "/~/globus-cli/jenkins-globus-online-workflow/error_file",
                "/~/jenkins-mule/error_file", False)
    os.system("cat /dev/null > error_file")
# end def my_task_list


def main():
    """
    main program
    """
    tokens = None
    try:
        # if we already have tokens, load and use them
        tokens = load_tokens_from_file(TOKEN_FILE)
    except IOError:
        pass

    if not tokens:
        # if we need to get tokens, start the Native App authentication process
        tokens = do_native_app_authentication(CLIENT_ID, REDIRECT_URI, SCOPES)

        try:
            save_tokens_to_file(TOKEN_FILE, tokens)
        except IOError:
            pass

    transfer_tokens = tokens['transfer.api.globus.org']

    auth_client = globus_sdk.NativeAppAuthClient(client_id=CLIENT_ID)

    authorizer = globus_sdk.RefreshTokenAuthorizer(
        transfer_tokens['refresh_token'],
        auth_client,
        access_token=transfer_tokens['access_token'],
        expires_at=transfer_tokens['expires_at_seconds'],
        on_refresh=update_tokens_file_on_refresh)

    tclient = globus_sdk.TransferClient(authorizer=authorizer)

    while True:
        # check for a jenkins_test_request file
        mylabel = "jenkins_test_request_check"
        print("WORKFLOW: ", mylabel)
        my_transfer(tclient, EP_BW, EP_OFFICEMAC, mylabel,
                    "/~/jenkins-mule/jenkins_test_request",
                    "/~/globus-cli/jenkins-globus-online-workflow/jenkins_test_request",
                    False)
        test_requested = os.path.exists('./jenkins_test_request')
        if test_requested:
            print("jenkins_test_request found at ncsa#BlueWaters, proceeding with test workflow...")
            # execute the GLobus Online test workflow
            os.system("date > status_file")
            my_task_list(tclient)
            os.system("rm ./jenkins_test_request")
            mylabel = "jenkinst_test_request_rm"
            print("WORKFLOW: ", mylabel)
            my_delete(tclient, EP_BW, mylabel,
                      "/~/jenkins-mule/jenkins_test_request", False)
            time.sleep(30)
            print("Workflow finished.")
        else:
            # just in case, normally this code will not be reached
            # since my_transfer waits for completion.
            # a canceled task may land you here
            print("Workflow waiting for jenkins_test_request.")
            time.sleep(60)
        # end while
# end def main()


main()
