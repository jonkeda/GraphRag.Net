using GraphRag.Net.Domain.Interface;
using GraphRag.Net.Domain.Model.Graph;
using Microsoft.AspNetCore.Mvc;

namespace GraphRag.Net.Api.Controllers;

[Route("api/[controller]/[action]")]
[ApiController]
public class GraphController(IGraphService _graphService) : ControllerBase
{
    /// <summary>
    /// Get all index keys
    /// </summary>
    /// <returns></returns>
    [HttpGet]
    public async Task<IActionResult> GetAllIndex()
    {
        var graphModel = _graphService.GetAllIndex();
        return Ok(graphModel);
    }


    /// <summary>
    /// Get all graph data
    /// </summary>
    /// <param name="index">Index key</param>
    /// <returns></returns>
    [HttpGet]
    public async Task<IActionResult> GetAllGraphs(string index)
    {
        if (string.IsNullOrEmpty(index))
        {
            return Ok(new GraphViewModel());
        }
        var graphModel = _graphService.GetAllGraphs(index);
        return Ok(graphModel);
    }


    /// <summary>
    /// Insert raw text (will trigger graph generation)
    /// </summary>
    /// <param name="model">Input model</param>
    /// <returns></returns>
    [HttpPost]
    public async Task<IActionResult> InsertGraphData(InputModel model)
    {
        await _graphService.InsertGraphDataAsync(model.Index, model.Input);
        return Ok();
    }

    /// <summary>
    /// Search recursively for related nodes/edges and perform graph based QA
    /// </summary>
    /// <param name="model">Input model</param>
    /// <returns></returns>
    [HttpPost]
    public async Task<IActionResult> SearchGraph(InputModel model)
    {
        var result = await _graphService.SearchGraphAsync(model.Index, model.Input);
        return Ok(result);
    }

    /// <summary>
    /// Search via community algorithm for dialogue
    /// </summary>
    /// <param name="model">Input model</param>
    /// <returns></returns>
    [HttpPost]
    public async Task<IActionResult> SearchGraphCommunity(InputModel model)
    {
        var result = await _graphService.SearchGraphCommunityAsync(model.Index, model.Input);
        return Ok(result);
    }

    /// <summary>
    /// Import txt document
    /// </summary>
    /// <param name="index">Index key</param>
    /// <param name="file">Uploaded file</param>
    /// <returns></returns>
    [HttpPost]
    public async Task<IActionResult> ImportTxt(string index,IFormFile file)
    {
        var forms = await Request.ReadFormAsync();
        using (var stream = new StreamReader(file.OpenReadStream()))
        {
            var txt = await stream.ReadToEndAsync();
            await _graphService.InsertTextChunkAsync(index,txt);
            return Ok();
        }
    }

    /// <summary>
    /// Generate communities and summaries via community detection
    /// </summary>
    /// <param name="index">Index key</param>
    /// <returns></returns>
    [HttpGet]
    public async Task<IActionResult> GraphCommunities(string index)
    {
        await _graphService.GraphCommunitiesAsync(index);
        return Ok();
    }      
        
    /// <summary>
    /// Generate global summary from community summaries
    /// </summary>
    /// <param name="index">Index key</param>
    /// <returns></returns>
    [HttpGet]
    public async Task<IActionResult> GraphGlobal(string index)
    {
        await _graphService.GraphGlobalAsync(index);
        return Ok();
    }

    /// <summary>
    /// Delete all graph data of an index
    /// </summary>
    /// <param name="index">Index key</param>
    /// <returns></returns>
    [HttpGet]
    public async Task<IActionResult> DeleteGraph(string index)
    {
        await _graphService.DeleteGraph(index);
        return Ok();
    }
}

public class InputModel
{
    public string Index { get; set; }
    public string Input { get; set; }
}