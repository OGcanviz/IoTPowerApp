using Newtonsoft.Json;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

class EmailService
{
    private HttpClient httpClient;
    private string sendEmailFunctionURL;

    public EmailService(string sendEmailFunctionURL)
    {
        this.httpClient = new HttpClient();
        this.sendEmailFunctionURL = sendEmailFunctionURL;
    }

    public async Task SendAsync(string[] to, string subject, string body)
    {
        var json = JsonConvert.SerializeObject(new
        {
            to = string.Join(";", to),
            subject,
            body
        });
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        var responseMessage = await httpClient.PostAsync(sendEmailFunctionURL, content);
        responseMessage.EnsureSuccessStatusCode();
    }
}