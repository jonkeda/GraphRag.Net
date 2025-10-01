using AntDesign;
using AntDesign.ProLayout;
using GraphRag.Net.Web.Models;
using Microsoft.AspNetCore.Components;

namespace GraphRag.Net.Web.Components;

public partial class RightContent
{
    private CurrentUser _currentUser = new CurrentUser();
    private NoticeIconData[] _notifications = [];
    private NoticeIconData[] _messages = [];
    private NoticeIconData[] _events = [];
    private int _count = 0;

    private List<AutoCompleteDataItem<string>> DefaultOptions { get; set; } =
    [
        new AutoCompleteDataItem<string>
        {
            Label = "umi ui",
            Value = "umi ui"
        },

        new AutoCompleteDataItem<string>
        {
            Label = "Pro Table",
            Value = "Pro Table"
        },

        new AutoCompleteDataItem<string>
        {
            Label = "Pro Layout",
            Value = "Pro Layout"
        }
    ];

    public AvatarMenuItem[] AvatarMenuItems { get; set; } =
    [
        new() { Key = "center", IconType = "user", Option = "Personal Center"},
        new() { Key = "setting", IconType = "setting", Option = "Personal Settings"},
        new() { IsDivider = true },
        new() { Key = "logout", IconType = "logout", Option = "Logout"}
    ];

    [Inject] protected NavigationManager NavigationManager { get; set; }

    [Inject] protected MessageService MessageService { get; set; }

    protected override async Task OnInitializedAsync()
    {
        await base.OnInitializedAsync();
        SetClassMap();

    }

    protected void SetClassMap()
    {
        ClassMapper
            .Clear()
            .Add("right");
    }

    public void HandleSelectUser(MenuItem item)
    {
        switch (item.Key)
        {
            case "center":
                NavigationManager.NavigateTo("/account/center");
                break;
            case "setting":
                NavigationManager.NavigateTo("/account/settings");
                break;
            case "logout":
                NavigationManager.NavigateTo("/user/login");
                break;
        }
    }

    public void HandleSelectLang(MenuItem item)
    {
    }

    public async Task HandleClear(string key)
    {
        switch (key)
        {
            case "notification":
                _notifications = [];
                break;
            case "message":
                _messages = [];
                break;
            case "event":
                _events = [];
                break;
        }
        MessageService.Success($"Cleared {key}");
    }

    public async Task HandleViewMore(string key)
    {
        MessageService.Info("Click on view more");
    }
}