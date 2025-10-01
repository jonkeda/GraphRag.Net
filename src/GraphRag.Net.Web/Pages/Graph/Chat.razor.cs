using AntDesign;
using GraphRag.Net.Domain.Interface;
using Microsoft.AspNetCore.Components;

namespace GraphRag.Net.Web.Pages.Graph;

public partial class Chat
{
    [Inject] IGraphService _graphService { get; set; }
    [Inject] IMessageService _message { get; set; }
    private List<UploadFileItem> _fileList = [];
    private List<string> _indexList { get; set; }
    private bool loading = false;
    private string _index;
    private string _input;
    private string _output;

    private string _importIndex;
    private string _importText;

    protected override async Task OnInitializedAsync()
    {
        await base.OnInitializedAsync();
        _indexList = _graphService.GetAllIndex();
    }

    private async Task Search()
    {
        Console.Write("111");
        loading = true;
        _output = await _graphService.SearchGraphAsync(_index, _input);
        loading = false;
    }

    private async Task Search1()
    {
        loading = true;
        _output = await _graphService.SearchGraphCommunityAsync(_index, _input);
        loading = false;
    }


    private void OnSelectedItemChangedHandler(string value)
    {
        _index = value;
    }


    private  bool BeforeUpload(UploadFileItem file)
    {
        if (string.IsNullOrEmpty(_importIndex))
        {
            //_message.ErrorAsync("Please fill in the index first").get;
            return false;
        }
        if (file.Type != "text/plain")
        {
            //await _message.ErrorAsync("File format error, please select again!");
            return false;
        }
        if (file.Size > 1024 * 1024 * 100)
        {
            //await _message.ErrorAsync("File must be no larger than 100MB!");
            return false;
        }

        return true;
    }

    private async Task OnSingleCompleted(UploadInfo fileinfo)
    {
        _indexList = _graphService.GetAllIndex();
        await _message.InfoAsync("Import completed");
    }

    private async Task InputText()
    {
        if (string.IsNullOrEmpty(_importIndex))
        { 
             await _message.ErrorAsync("Please fill in the index first");
        }

        try
        {
            await _graphService.InsertGraphDataAsync(_importIndex, _importText);

            // Generate community and global summaries
            await _graphService.GraphCommunitiesAsync(_importIndex);
            await _graphService.GraphGlobalAsync(_importIndex);

            _indexList = _graphService.GetAllIndex();
             await _message.InfoAsync("Import completed");
        }
        catch (Exception e)
        {
            await _message.ErrorAsync(e.Message);
        }
    }
}